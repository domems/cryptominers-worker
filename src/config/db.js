import {neon} from "@neondatabase/serverless";

import "dotenv/config";

//Cria a conexão SQL usando o nosso URL da base de dados  v v v
export const sql = neon(process.env.DATABASE_URL);