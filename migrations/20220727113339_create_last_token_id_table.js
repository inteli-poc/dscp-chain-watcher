/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = async function(knex) {
    console.log('creating table')
    await knex.schema.createTable('blockchainlasttokenid', (def) => {
        def.integer('lasttokenidprocessed').notNullable()
        def.primary(['lasttokenidprocessed'])
      })
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = async function(knex) {
    console.log('dropping table')
    await knex.schema.dropTable('blockchainlasttokenid')
};
