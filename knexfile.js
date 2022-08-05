require('dotenv').config()
module.exports = {
  test: {
    client: 'pg',
    connection: {
      host: process.env.DB_HOST || 'localhost',
      database: process.env.DB_NAME || 'inteli',
      user: process.env.DB_USERNAME || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres',
      port: process.env.DB_PORT || '5432',
    },
    pool: {
      min: 2,
      max: 10,
    },
    migrations: {
      tableName: 'blockchainwatchermigrations',
    },
    seeds: {
      directory: './test/seeds',
    },
  },
  development: {
    client: 'pg',
    connection: {
      host: process.env.DB_HOST || 'localhost',
      database: process.env.DB_NAME || 'inteli',
      user: process.env.DB_USERNAME || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres',
      port: process.env.DB_PORT || '5432',
    },
    pool: {
      min: 2,
      max: 10,
    },
    migrations: {
      tableName: 'blockchainwatchermigrations',
    },
  },
  production: {
    client: 'pg',
    connection: {
      host: process.env.DB_HOST,
      database: process.env.DB_NAME,
      user: process.env.DB_USERNAME,
      password: process.env.DB_PASSWORD,
      port: process.env.DB_PORT,
    },
    pool: {
      min: 2,
      max: 10,
    },
    migrations: {
      tableName: 'blockchainwatchermigrations',
    },
  },
}
