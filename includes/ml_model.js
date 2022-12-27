function create_model(model_name, model_from, model_type, options) {
  return `
    CREATE MODEL \`${model_name}\`
    OPTIONS(
        MODEL_TYPE = '${model_type}',
        ${options.map((field) => `${field.name} = ${field.value}`).join(",\n\t\t")}
    )
    AS ${model_from}
  `
}

module.exports = { create_model };