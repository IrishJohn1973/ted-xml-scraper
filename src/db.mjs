import 'dotenv/config';
import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false } // fix: allow Supabase's cert chain
});

export const query = (text, params) => pool.query(text, params);
export { pool };
