WITH ENVOLVIDOS AS (
SELECT 
    env.numero_ocorrencia,
    SUBSTRING(env.numero_ocorrencia, 1, 14) AS REDS_ABREVIADO,
    env.nome_completo_envolvido,
    env.nome_mae,
    env.envolvimento_descricao,
    env.condicao_fisica_descricao, 
    CASE
	    WHEN env.codigo_sexo = 'M' THEN 'Masculino'
	    WHEN env.codigo_sexo = 'F' THEN 'Feminino'
	    ELSE 'Indeterminada'
	END AS codigo_sexo,
    env.valor_idade_aparente,
    env.cor_pele_descricao,
    env.codigo_uf_natural,
    env.nome_municipio,
    CASE 
    	WHEN env.ind_turista = 'S' THEN 'Sim'
    	WHEN env.ind_turista = 'N' THEN 'Não'
    	ELSE '-'
    END AS ind_turista,
    ROW_NUMBER() OVER (
        PARTITION BY 
            SUBSTRING(env.numero_ocorrencia, 1, 14), 
            env.nome_completo_envolvido, 
            env.nome_mae 
        ORDER BY env.numero_ocorrencia
    ) AS rn
FROM db_bisp_reds_reporting.tb_envolvido_ocorrencia env
WHERE 1=1
	AND env.unidade_responsavel_registro_codigo LIKE 'B%'
	AND YEAR(env.data_hora_fato) = :ANO_FATO 
	AND env.envolvimento_codigo LIKE '13%'
)

SELECT 
    numero_ocorrencia,
    envolvimento_descricao,
    condicao_fisica_descricao,
    codigo_sexo,
    valor_idade_aparente,
    cor_pele_descricao,
    codigo_uf_natural,
    nome_municipio,
    ind_turista
FROM ENVOLVIDOS
WHERE rn = 1;