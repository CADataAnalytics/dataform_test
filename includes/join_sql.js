function crie_relacionamento(fields1, table1, chave1, fields2, table2, chave2, quando) {
  return `
    SELECT 
      ${fields1.map((field1) => `tb1.${field1}`).join(", ")},
      ${fields2.map((field2) => `tb2.${field2}`).join(", ")}
    FROM ${table1} as tb1
    JOIN ${table2} as tb2
    ON tb1.${chave1} = tb2.${chave2}
    WHERE 
      ${quando.map((quando) => quando).join(" AND ")}
  `
}

module.exports = { crie_relacionamento };