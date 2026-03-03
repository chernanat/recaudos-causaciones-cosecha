// job.js — Causaciones (GET) firma FULL + APIs BigQuery (NITs) + cosecha e inserción BQ
// Igual que index.js pero sin Express: corre la cosecha directo y termina.
// Configuración por variables de entorno (mismas que antes + las del Job abajo).
import crypto from "node:crypto";
import { BigQuery } from "@google-cloud/bigquery";

// ================== Config SER ==================
const BASE_URL = process.env.BASE_URL_SER || "https://ser.mintic.gov.co/SER.API";
const VERSION = process.env.VERSION_SER || "/api/v1/";
const SERVICIO = process.env.SERVICIO_SER || "ConsultaExterna/ConsultarCausaciones";

// ================== Config BigQuery ==================
const BQ_PROJECT = process.env.BQ_PROJECT || "mintic-models-prod";
const BQ_LOCATION = process.env.BQ_LOCATION || "US";
const VISTA_BDU = process.env.BQ_VISTA_BDU || "`mintic-models-prod.contraprestaciones_pro.SER_BDU_PERIODICA`";
const TABLA_DESTINO_FULL = process.env.BQ_TABLA_DESTINO || "mintic-models-prod.contraprestaciones_pro.SER_Causaciones_prod";

const bigquery = new BigQuery({
    projectId: "mintic-models-prod",
});

// ================== Contador global de timestamp ==================
const _lastTimestampPerConsumidor = new Map();

async function getUniqueTimestamp(consumidor) {
    const key = String(consumidor);
    const nowSec = Math.floor(Date.now() / 1000);
    const last = _lastTimestampPerConsumidor.get(key) ?? 0;

    if (nowSec > last) {
        _lastTimestampPerConsumidor.set(key, nowSec);
        return nowSec;
    }

    const waitMs = (last + 1) * 1000 - Date.now() + 10;
    if (waitMs > 0) {
        await new Promise(res => setTimeout(res, waitMs));
    }
    const newSec = Math.floor(Date.now() / 1000);
    _lastTimestampPerConsumidor.set(key, newSec);
    return newSec;
}

// ================== Helpers comunes ==================
function ensureLeadingSlash(s) {
    if (!s) return "";
    if (s.startsWith("/") || s.startsWith("?")) return s;
    return `/${s}`;
}
function pathFromSegments(...segments) {
    const cleaned = segments
        .filter((s) => s != null && `${s}`.length > 0)
        .map((s) => encodeURIComponent(String(s).trim()));
    return cleaned.length ? `/${cleaned.join("/")}` : "";
}
function splitPathAndQuery(p) {
    const s = ensureLeadingSlash(p || "");
    if (!s) return { pathOnly: "", query: "" };
    const i = s.indexOf("?");
    if (i === -1) return { pathOnly: s, query: "" };
    return { pathOnly: s.slice(0, i), query: s.slice(i) };
}
function esErrorAnnioNoNumerico(resp) {
    const desc = String(resp?.data?.descripcion || resp?.descripcion || "");
    return resp?.status === 200 && /annio/i.test(desc) && /num[eé]ric/i.test(desc);
}

// ================== Firma FULL con timestamp único ==================
async function firmarFULL(parametros, consumidor, llave) {
    const { pathOnly, query } = splitPathAndQuery(parametros || "");
    const rutaControlador = `${VERSION}${SERVICIO}${pathOnly ? ensureLeadingSlash(pathOnly) : ""}${query || ""}`;

    const timestamp = await getUniqueTimestamp(consumidor);

    const baseString = `${consumidor}:GET:${rutaControlador}:${timestamp}`;
    const token = crypto.createHmac("sha256", Buffer.from(llave, "utf8"))
        .update(Buffer.from(baseString, "utf8"))
        .digest("base64");
    const tokenHeader = `consumidor=${consumidor};timestamp=${timestamp};token=${token}`;
    return {
        headers: { "SER-INTEROP-TOKEN": tokenHeader, Accept: "application/json", "Content-Type": "application/json" },
        debug: { baseString, rutaControlador, timestamp },
    };
}

async function fetchSER(url, headers) {
    const r = await fetch(url, { method: "GET", headers });
    const text = await r.text();
    let data; try { data = JSON.parse(text); } catch { data = text; }
    return {
        ok: r.ok, status: r.status, statusText: r.statusText,
        headers: Object.fromEntries(r.headers.entries()),
        urlLlamada: url, data
    };
}

async function intentoGET(parametros, consumidor, llave, etiqueta) {
    const { headers, debug } = await firmarFULL(parametros, consumidor, llave);
    const url = `${BASE_URL}${VERSION}${SERVICIO}${ensureLeadingSlash(parametros)}`;
    const resp = await fetchSER(url, headers);

    if (resp.status === 401 || resp.status === 403) {
        console.warn(`[SER_AUTH_ERROR] ${etiqueta} | Status: ${resp.status} | URL: ${url} | Consumidor: ${consumidor}`);
        console.warn(`[SER_AUTH_ERROR] Token Header: ${headers["SER-INTEROP-TOKEN"]}`);
        console.warn(`[SER_AUTH_ERROR] BaseString: ${debug.baseString}`);
    }

    return { resp, intento: { modo: etiqueta, url, parametros, status: resp.status, firma: debug } };
}

async function consultarCausaciones(nit, anio, consumidor, llave) {
    const intentos = [];

    // 1) /{annio}/{nit}
    {
        const parametros = pathFromSegments(anio, nit);
        const { resp, intento } = await intentoGET(parametros, consumidor, llave, "GET:path annio/nit (FULL)");
        intentos.push(intento);
        if (resp.status !== 404 && !esErrorAnnioNoNumerico(resp)) {
            resp._modoLlamada = intento.modo; resp._intentos = intentos; return resp;
        }
    }
    // 2) /{nit}/{annio}
    {
        const parametros = pathFromSegments(nit, anio);
        const { resp, intento } = await intentoGET(parametros, consumidor, llave, "GET:path nit/annio (FULL)");
        intentos.push(intento);
        if (resp.status !== 404 && !esErrorAnnioNoNumerico(resp)) {
            resp._modoLlamada = intento.modo; resp._intentos = intentos; return resp;
        }
    }
    // 3) ?nit=&annio=
    {
        const parametros = `?nit=${encodeURIComponent(nit)}&annio=${encodeURIComponent(anio)}`;
        const { resp, intento } = await intentoGET(parametros, consumidor, llave, "GET:query annio (FULL+query)");
        intentos.push(intento);
        if (resp.status !== 404 && !esErrorAnnioNoNumerico(resp)) {
            resp._modoLlamada = intento.modo; resp._intentos = intentos; return resp;
        }
    }
    // 4) ?nit=&anio=
    {
        const parametros = `?nit=${encodeURIComponent(nit)}&anio=${encodeURIComponent(anio)}`;
        const { resp, intento } = await intentoGET(parametros, consumidor, llave, "GET:query anio (FULL+query)");
        intentos.push(intento);
        if (resp.status !== 404 && !esErrorAnnioNoNumerico(resp)) {
            resp._modoLlamada = intento.modo; resp._intentos = intentos; return resp;
        }
    }
    // 5) ?nitOperador=&annio=
    {
        const parametros = `?nitOperador=${encodeURIComponent(nit)}&annio=${encodeURIComponent(anio)}`;
        const { resp, intento } = await intentoGET(parametros, consumidor, llave, "GET:query annio (nitOperador)");
        intentos.push(intento);
        if (resp.status !== 404 && !esErrorAnnioNoNumerico(resp)) {
            resp._modoLlamada = intento.modo; resp._intentos = intentos; return resp;
        }
    }
    // 6) ?nitOperador=&anio=
    {
        const parametros = `?nitOperador=${encodeURIComponent(nit)}&anio=${encodeURIComponent(anio)}`;
        const { resp, intento } = await intentoGET(parametros, consumidor, llave, "GET:query anio (nitOperador)");
        intentos.push(intento);
        resp._modoLlamada = intento.modo; resp._intentos = intentos; return resp;
    }
}

// ================== Helpers BigQuery (NITs) ==================
function normalizarNitLocal(n) {
    return String(n || "").replace(/[^\d]/g, "").trim();
}

async function obtenerNitsPaginados(page = 1, pageSize = 100) {
    const safePage = Number.isFinite(page) && page > 0 ? page : 1;
    const safePageSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.min(pageSize, 1000) : 100;
    const offset = (safePage - 1) * safePageSize;

    const sql = `
    WITH base AS (
      SELECT DISTINCT
        REGEXP_REPLACE(TRIM(CAST(Identificacion AS STRING)), r'[^0-9]', '') AS nit
      FROM ${VISTA_BDU}
      WHERE Tipo_Identificacion = 'NIT'
        AND Aplica_Cobro = 'SI'
    ),
    ordenado AS (
      SELECT nit FROM base
      WHERE nit IS NOT NULL AND nit != ''
    )
    SELECT
      nit,
      COUNT(*) OVER() AS total
    FROM ordenado
    ORDER BY LENGTH(nit), nit
    LIMIT @pageSize OFFSET @offset
  `;

    const [job] = await bigquery.createQueryJob({
        query: sql,
        location: BQ_LOCATION,
        params: { pageSize: safePageSize, offset }
    });
    const [rows] = await job.getQueryResults();

    const nits = rows.map(r => normalizarNitLocal(r.nit)).filter(Boolean);
    const totalRegistros = Number(rows[0]?.total ?? 0);
    const totalPaginas = totalRegistros > 0 ? Math.ceil(totalRegistros / safePageSize) : 0;

    return { page: safePage, pageSize: safePageSize, totalRegistros, totalPaginas, nits };
}

async function obtenerTodosLosNits(pageSize = 300) {
    const primero = await obtenerNitsPaginados(1, pageSize);
    const todos = [...primero.nits];
    for (let p = 2; p <= primero.totalPaginas; p++) {
        const { nits } = await obtenerNitsPaginados(p, pageSize);
        todos.push(...nits);
    }
    return { totalRegistros: primero.totalRegistros, nits: todos };
}

// ================== Helpers BigQuery (Inserción) ==================
function parseTableRef(fq) {
    const parts = String(fq).split(".");
    if (parts.length === 3) return { project: parts[0], dataset: parts[1], table: parts[2] };
    if (parts.length === 2) return { project: BQ_PROJECT, dataset: parts[0], table: parts[1] };
    throw new Error("BQ_TABLA_DESTINO inválida. Usa 'proyecto.dataset.tabla' o 'dataset.tabla'.");
}
const { project: DEST_PROJECT, dataset: DEST_DATASET, table: DEST_TABLE } = parseTableRef(TABLA_DESTINO_FULL);
const tablaDestino = (DEST_PROJECT === BQ_PROJECT ? bigquery : new BigQuery({ projectId: DEST_PROJECT }))
    .dataset(DEST_DATASET)
    .table(DEST_TABLE);

function toISOorNull(v) {
    if (!v) return null;
    const d = new Date(v);
    return isNaN(d.getTime()) ? null : d.toISOString();
}
function toNumericStringOrNull(v) {
    if (v === null || v === undefined) return null;
    return String(v);
}

function mapearFilaSER(respJson) {
    const j = respJson?.data || {};
    const arr = Array.isArray(j.data) ? j.data : [];

    return {
        data: arr.map(it => ({
            tipoPresentacion: it?.tipoPresentacion ?? null,
            referenciaFUR: it?.referenciaFUR ?? null,
            nitOperador: it?.nitOperador ?? null,
            nombreRazonSocial: it?.nombreRazonSocial ?? null,
            fechaPresentacion: toISOorNull(it?.fechaPresentacion),
            fechaOperativa: toISOorNull(it?.fechaOperativa),
            fechaProceso: toISOorNull(it?.fechaProceso),
            numeroExpediente: it?.numeroExpediente ?? null,
            codigoServicio: it?.codigoServicio ?? null,
            nombreServicio: it?.nombreServicio ?? null,
            fechaInicial: toISOorNull(it?.fechaInicial),
            fechaFinal: toISOorNull(it?.fechaFinal),
            listarConceptos: Array.isArray(it?.listarConceptos)
                ? it.listarConceptos.map(c => ({
                    codigoConcepto: c?.codigoConcepto ?? null,
                    nombreConcepto: c?.nombreConcepto ?? null,
                    valorConcepto: toNumericStringOrNull(c?.valorConcepto)
                }))
                : []
        })),
        dataErroresValidacionAPI: j?.dataErroresValidacionAPI ?? null,
        codigoError: j?.codigoError ?? null,
        fecha: toISOorNull(j?.fecha),
        descripcion: j?.descripcion ?? null,
        mostrarTicket: j?.mostrarTicket ?? null,
        ticket: j?.ticket ?? null,
        estado: j?.estado ?? null,
        tipoIconoMensaje: j?.tipoIconoMensaje ?? null,
        FECHA_EJECUCION: new Date().toISOString()
    };
}

function buildInsertId(row) {
    const h = crypto.createHash("sha256").update(JSON.stringify(row)).digest("hex");
    return h.slice(0, 32);
}

async function insertarFilasBQ(filas) {
    if (!filas?.length) return { inserted: 0, errors: [] };
    const rowsForBQ = filas.map(r => ({ insertId: buildInsertId(r), json: r }));
    try {
        await tablaDestino.insert(rowsForBQ, { raw: true, ignoreUnknownValues: false });
        return { inserted: filas.length, errors: [] };
    } catch (e) {
        const perRow = Array.isArray(e?.errors) ? e.errors : [];
        const inserted = filas.length - perRow.length;
        return { inserted: Math.max(inserted, 0), errors: perRow };
    }
}

// ================== Helpers de fecha / TZ ==================
function getYmdInTZ(date, timeZone = "America/Bogota") {
    const f = new Intl.DateTimeFormat("en-CA", {
        timeZone, year: "numeric", month: "2-digit", day: "2-digit"
    });
    const parts = f.formatToParts(date);
    const y = Number(parts.find(p => p.type === "year").value);
    const m = Number(parts.find(p => p.type === "month").value);
    const d = Number(parts.find(p => p.type === "day").value);
    return { y, m, d };
}

function sameCalendarDayInTZ(isoLike, refDate = new Date(), timeZone = "America/Bogota") {
    if (!isoLike) return false;
    const d = new Date(isoLike);
    if (isNaN(d.getTime())) return false;
    const a = getYmdInTZ(d, timeZone);
    const b = getYmdInTZ(refDate, timeZone);
    return a.y === b.y && a.m === b.m && a.d === b.d;
}

// ================== Concurrencia simple ==================
async function runPool(items, limit, worker) {
    const results = [];
    let i = 0;
    const running = new Set();
    async function runOne(idx) {
        const item = items[idx];
        try {
            const r = await worker(item, idx);
            results[idx] = r;
        } catch (err) {
            results[idx] = { error: String(err?.message || err) };
        } finally {
            running.delete(idx);
            if (i < items.length) {
                const next = i++;
                running.add(next);
                runOne(next);
            }
        }
    }
    const first = Math.min(limit, items.length);
    for (; i < first; i++) {
        running.add(i);
        runOne(i);
    }
    while (running.size) {
        await new Promise(res => setTimeout(res, 25));
    }
    return results;
}

// ================== MAIN ==================
// Variables de entorno del Job (equivalen a los query params del scheduler anterior):
//   JOB_ANIO        → anio=2026
//   JOB_ANIOS       → anios=2025,2026  (varios años, tiene precedencia sobre JOB_ANIO)
//   JOB_MODO        → "full" (todos) | "hoy" (solo fechaPresentacion=hoy). Default: full
//   JOB_INSERT      → insert=1 | 0. Default: 1
//   JOB_CONCURRENCY → concurrency=3. Default: 3
//   JOB_PAGE_SIZE   → pageSize=300. Default: 300
//   JOB_TZ          → tz=America/Bogota. Default: America/Bogota
async function main() {
    const consumidor = (process.env.CONSUMIDOR_SER || "").trim();
    const llave = (process.env.LLAVE_CONSUMIDOR_SER || "").trim();
    if (!consumidor || !llave) {
        console.error("[JOB_ERROR] Faltan variables de entorno: CONSUMIDOR_SER y/o LLAVE_CONSUMIDOR_SER");
        process.exit(1);
    }

    const modo        = (process.env.JOB_MODO        || "full").toLowerCase();
    const tz          = process.env.JOB_TZ            || "America/Bogota";
    const doInsert    = (process.env.JOB_INSERT       ?? "1") === "1";
    const concurrency = Math.max(1, Math.min(Number(process.env.JOB_CONCURRENCY || "3"), 24));
    const pageSize    = Math.max(1, Math.min(Number(process.env.JOB_PAGE_SIZE   || "300"), 1000));

    // Resolver años
    let anios = [];
    if (process.env.JOB_ANIOS) {
        anios = String(process.env.JOB_ANIOS).split(",").map(a => a.trim()).filter(a => /^\d{4}$/.test(a));
    } else if (process.env.JOB_ANIO && /^\d{4}$/.test(String(process.env.JOB_ANIO))) {
        anios = [String(process.env.JOB_ANIO)];
    } else if (modo === "hoy") {
        anios = [String(getYmdInTZ(new Date(), tz).y)];
    } else {
        console.error("[JOB_ERROR] Define JOB_ANIO o JOB_ANIOS con el año a cosechar.");
        process.exit(1);
    }

    console.log(`[JOB_START] ${new Date().toISOString()} | modo=${modo} | anios=${anios.join(",")} | concurrency=${concurrency} | pageSize=${pageSize} | insert=${doInsert}`);

    const { totalRegistros, nits } = await obtenerTodosLosNits(pageSize);
    console.log(`[JOB_NITS] Total BDU: ${totalRegistros} | Procesando: ${nits.length}`);

    const trabajos = [];
    for (const anio of anios) for (const nit of nits) trabajos.push({ nit, anio });
    console.log(`[JOB_TRABAJOS] Combinaciones NIT×AÑO: ${trabajos.length}`);

    const includeData = false; // el scheduler anterior usaba includeData=0
    const hoy = new Date();

    const resultados = await runPool(trabajos, concurrency, async (t) => {
        const r = await consultarCausaciones(t.nit, t.anio, consumidor, llave);
        const j = r?.data || {};
        const arr = Array.isArray(j?.data) ? j.data : [];

        const arrUsada = modo === "hoy"
            ? arr.filter(it => sameCalendarDayInTZ(it?.fechaPresentacion, hoy, tz))
            : arr;

        return {
            nit: t.nit,
            anio: Number(t.anio),
            statusSER: r.status,
            registros: arrUsada.length,
            modo: r._modoLlamada,
            descripcionSER: r.statusText,
            _arrUsada: arrUsada,
            _j: j,
        };
    });

    // Preparar filas para BQ (igual que /cosechar-causaciones-todos con includeData=0)
    const filasAInsertar = [];
    for (const item of resultados) {
        if (item.registros > 0) {
            let jsonSER;
            if (includeData) {
                jsonSER = { data: { data: item._arrUsada, dataErroresValidacionAPI: null, codigoError: null, fecha: null, descripcion: null, mostrarTicket: null, ticket: null, estado: null, tipoIconoMensaje: null } };
            } else {
                const r2 = await consultarCausaciones(item.nit, String(item.anio), consumidor, llave);
                jsonSER = { data: r2?.data };
            }
            const fila = mapearFilaSER(jsonSER);
            filasAInsertar.push(fila);
        }
    }

    let inserted = 0;
    let erroresInsert = [];
    if (doInsert && filasAInsertar.length) {
        console.log(`[JOB_INSERT] Insertando ${filasAInsertar.length} filas en ${DEST_DATASET}.${DEST_TABLE}...`);
        const { inserted: ins, errors } = await insertarFilasBQ(filasAInsertar);
        inserted = ins;
        erroresInsert = errors;
    }

    const conDatos = resultados.filter(x => x.registros > 0).length;
    const sinDatos = resultados.length - conDatos;
    const con401   = resultados.filter(x => x.statusSER === 401).length;
    const con403   = resultados.filter(x => x.statusSER === 403).length;

    console.log(`[JOB_RESULTADO] Total: ${resultados.length} | Con datos: ${conDatos} | Sin datos: ${sinDatos} | 401: ${con401} | 403: ${con403}`);
    console.log(`[JOB_INSERT] Filas preparadas: ${filasAInsertar.length} | Insertadas: ${inserted} | Errores: ${erroresInsert.length}`);
    if (erroresInsert.length) console.error("[JOB_INSERT_ERRORS]", JSON.stringify(erroresInsert.slice(0, 10), null, 2));

    console.log(`[JOB_END] ${new Date().toISOString()}`);
    process.exit(0);
}

main().catch(err => {
    console.error("[JOB_FATAL]", err);
    process.exit(1);
});