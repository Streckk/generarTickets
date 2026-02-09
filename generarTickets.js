#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { MongoClient } = require("mongodb");

function toBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "si", "on"].includes(normalized);
}

function toNumber(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value || !String(value).trim()) {
    throw new Error(`Variable de entorno requerida: ${name}`);
  }
  return String(value).trim();
}

function resolveTicketNumber(doc, nextTicketRef) {
  const raw = doc?.num_caso;
  if (raw !== undefined && raw !== null && String(raw).trim() !== "") {
    const parsed = Number.parseInt(String(raw).trim(), 10);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  nextTicketRef.value += 1;
  return nextTicketRef.value;
}

function buildTicketFromMsgStruct(doc, ticketNumber) {
  const createdAt = doc?.date_creation
    ? new Date(doc.date_creation)
    : new Date();
  const emailFrom = doc?.from?.email ? String(doc.from.email).trim() : "";
  const nameFrom = doc?.from?.name ? String(doc.from.name).trim() : "";
  const subject = doc?.subject ? String(doc.subject) : "";
  const statusFromMsg = doc?.Estatus ? String(doc.Estatus).trim() : "";
  const categoryFromMsg = doc?.Categoria ? String(doc.Categoria).trim() : "";
  const subCategoryFromMsg = doc?.Subcategoria
    ? String(doc.Subcategoria).trim()
    : "";
  const numberLinesFromMsg = doc?.Numero_lineas
    ? String(doc.Numero_lineas).trim()
    : "";
  const rawAssigned = Array.isArray(doc?.Agente_asignado)
    ? doc.Agente_asignado
    : doc?.Agente_asignado
      ? [doc.Agente_asignado]
      : [];
  const assignedFromMsg = rawAssigned
    .map((agent) => {
      if (typeof agent === "string") {
        return { user: agent.trim(), first_names: "", last_names: "" };
      }
      const user = agent?.user ?? agent?.usuario ?? agent?.email ?? "";
      const firstNames =
        agent?.first_names ?? agent?.first_name ?? agent?.nombre ?? "";
      const lastNames =
        agent?.last_names ?? agent?.last_name ?? agent?.apellidos ?? "";
      return {
        user: String(user || "").trim(),
        first_names: String(firstNames || "").trim(),
        last_names: String(lastNames || "").trim(),
      };
    })
    .filter((agent) => agent.user || agent.first_names || agent.last_names);

  return {
    ots_ticket: ticketNumber,
    ots_date_registration: createdAt,
    ots_date_modify: createdAt,
    ots_type: "externo",
    ots_user_create: emailFrom || "script",
    ots_name_client: nameFrom || "Sin nombre",
    ots_email_from: emailFrom || "sin-correo",
    ots_email_subject: subject,
    ots_who_close: "",
    ots_aditional_comments: "",
    ots_assigned: assignedFromMsg,
    ots_status: statusFromMsg || "Estatus",
    ots_category: categoryFromMsg,
    ots_sub_category: subCategoryFromMsg,
    ots_number_lines: numberLinesFromMsg,
    ots_priority: "Media",
    ots_source: "correo",
    ots_sla_minutes: 0,
    ots_is_closed: false,
  };
}

async function main() {
  const mongoUrl = requiredEnv("SRV_MONGO_MASTER_URL");
  const dbExchangeName = requiredEnv("SRV_MONGO_DB_EXCHANGE");
  const dbOperationsName = requiredEnv("SRV_MONGO_DB_OPERATIONS");

  const sourceCollectionName =
    process.env.SOURCE_COLLECTION || DEFAULT_SOURCE_COLLECTION;
  const targetCollectionName =
    process.env.TARGET_COLLECTION || DEFAULT_TARGET_COLLECTION;
  const dryRun = toBoolean(process.env.DRY_RUN, false);
  const limit = toNumber(process.env.LIMIT, 0);
  const batchSize = Math.max(1, toNumber(process.env.BATCH_SIZE, 10));
  const sourceQuery = {
    $and: [
      { message_type: "entrada" },
      { num_caso: { $exists: true, $nin: [null, ""] } },
    ],
  };
  const projection = {
    _id: 1,
    date_creation: 1,
    from: 1,
    subject: 1,
    conversation: 1,
    num_caso: 1,
    message_type: 1,
    Estatus: 1,
    Categoria: 1,
    Subcategoria: 1,
    Numero_lineas: 1,
    Agente_asignado: 1,
  };

  const client = new MongoClient(mongoUrl);
  await client.connect();

  try {
    const dbExchange = client.db(dbExchangeName);
    const dbOperations = client.db(dbOperationsName);
    const sourceCollection = dbExchange.collection(sourceCollectionName);
    const targetCollection = dbOperations.collection(targetCollectionName);

    const existingTickets = await targetCollection.distinct("ots_ticket");
    const existingSet = new Set(
      existingTickets
        .map((v) => Number.parseInt(String(v), 10))
        .filter((v) => Number.isFinite(v) && v > 0),
    );

    let maxTicket = 0;
    for (const ticket of existingSet) {
      if (ticket > maxTicket) maxTicket = ticket;
    }
    const nextTicketRef = { value: maxTicket };

    const cursor = sourceCollection
      .find(sourceQuery, { projection })
      .sort({ date_creation: 1 })
      .batchSize(batchSize);

    const toInsertBatch = [];
    let scanned = 0;
    let skippedDuplicates = 0;
    let generatedWithoutNumCaso = 0;
    let readyToInsert = 0;
    let insertedCount = 0;

    const flushBatch = async () => {
      if (toInsertBatch.length === 0) return;

      if (dryRun) {
        readyToInsert += toInsertBatch.length;
        toInsertBatch.length = 0;
        return;
      }

      const result = await targetCollection.insertMany(toInsertBatch, {
        ordered: false,
      });
      insertedCount += result.insertedCount || 0;
      toInsertBatch.length = 0;
    };

    while (await cursor.hasNext()) {
      const doc = await cursor.next();
      if (!doc) break;
      scanned += 1;

      const ticketNumber = resolveTicketNumber(doc, nextTicketRef);
      if (!doc?.num_caso) {
        generatedWithoutNumCaso += 1;
      }

      if (existingSet.has(ticketNumber)) {
        skippedDuplicates += 1;
        continue;
      }

      const ticket = buildTicketFromMsgStruct(doc, ticketNumber);
      toInsertBatch.push(ticket);
      existingSet.add(ticketNumber);

      const currentPrepared = dryRun
        ? readyToInsert + toInsertBatch.length
        : insertedCount + toInsertBatch.length;

      if (toInsertBatch.length >= batchSize) {
        await flushBatch();
      }

      if (limit > 0 && currentPrepared >= limit) {
        break;
      }
    }

    if (limit > 0) {
      const remaining = limit - (dryRun ? readyToInsert : insertedCount);
      if (remaining > 0 && toInsertBatch.length > remaining) {
        toInsertBatch.length = remaining;
      }
    }

    await flushBatch();

    if (dryRun) {
      console.info("[DRY_RUN] No se insertaron documentos.");
      console.info(
        JSON.stringify(
          {
            scanned,
            readyToInsert,
            skippedDuplicates,
            generatedWithoutNumCaso,
            batchSize,
            source: `${dbExchangeName}.${sourceCollectionName}`,
            target: `${dbOperationsName}.${targetCollectionName}`,
          },
          null,
          2,
        ),
      );
      return;
    }

    console.info(
      JSON.stringify(
        {
          scanned,
          insertedCount,
          skippedDuplicates,
          generatedWithoutNumCaso,
          batchSize,
          source: `${dbExchangeName}.${sourceCollectionName}`,
          target: `${dbOperationsName}.${targetCollectionName}`,
        },
        null,
        2,
      ),
    );
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error("[generarTickets] Error:", error.message || error);
  process.exit(1);
});
