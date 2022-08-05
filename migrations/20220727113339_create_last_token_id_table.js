/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = async function(knex) {
    await knex.schema.createTable('blockchainlasttokenid', (def) => {
        def.integer('lasttokenidprocessed').notNullable().default(0)
        def.primary(['lasttokenidprocessed'])
      })
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = async function(knex) {
    await knex.schema.dropTable('blockchainlasttokenid')
};
