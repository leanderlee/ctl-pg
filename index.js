const ctl = require('ctl');
const Pool = require('pg-pool');
const log = ctl.library('logging')('pg');
const config = ctl.library('config');

let pool;
const {
  host = 'localhost',
  port = 5432,
  database = 'db',
} = config.pg;

const options = {
  host,
  port: Number(port),
  database,
  max: 20,
  min: 1,
  idleTimeoutMillis: 1000, // close idle clients after 1s
  connectionTimeoutMillis: 1000, // return an error after 1s if no connection
};

if (config.pg.user) {
  options.user = config.pg.user;
  options.password = config.pg.password;
  options.ssl = !!config.pg.ssl;
}

async function query(sql, args) {
  const client = await pool.connect();
  let result = null;
  let error = null;
  try {
    result = await client.query(sql, args);
  } catch (e) {
    error = e;
  }
  client.release();
  if (error) {
    log.error(error);
    throw error;
  }
  return result;
}

const META_KEY = 'versions';
const META_TABLE = 'metainfo';

async function hasTable() {
  const { rowCount } = await query(`
    SELECT table_name
    FROM information_schema.tables
    WHERE  table_schema = $1::text
    AND    table_name = $2::text;
  `, ['public', META_TABLE]);
  return (rowCount > 0);
}

exports.connect = async () => {
  log.info('Connect DB (pg://%s:%s):', host, port, database);
  pool = new Pool(options);
};

exports.pool = () => pool;
exports.query = query;

ctl.connect(exports.connect);
ctl.metainfo(async () => {
  if (!pool) return;
  return {
    set: async (val) => {
      await query(`
        INSERT INTO ${META_TABLE} (setting, value)
        VALUES ('${META_KEY}', $1::text)
        ON CONFLICT DO NOTHING
      `, [JSON.stringify(val)]);
      await query(`
        UPDATE ${META_TABLE}
        SET value = $1
        WHERE setting = '${META_KEY}'
      `, [JSON.stringify(val)]);
    },
    get: async () => {
      if (!await hasTable()) {
        await query(`
          CREATE TABLE IF NOT EXISTS ${META_TABLE} (setting text unique, value text)
        `);
      }
      const { rowCount, rows } = await query(`
        SELECT value FROM ${META_TABLE} WHERE setting = '${META_KEY}'
      `);
      if (rowCount === 0) return;
      return JSON.parse(rows[0].value);
    },
  };
});
