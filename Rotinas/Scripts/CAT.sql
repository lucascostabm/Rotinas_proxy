WITH 
PREVENCAO AS (
SELECT DISTINCT 
	prev.numero_ocorrencia,
	prev.numero_projeto,
	prev.ocupacao_divisao_codigo,
	prev.ocupacao_divisao_descricao,
    CASE 
        WHEN prev.id_classif_risco_cor = 9 THEN 'ALTO RISCO'
        WHEN prev.id_classif_risco_cor = 8 THEN 'BAIXO RISCO'
        WHEN prev.id_classif_risco_cor = 7 THEN 'DISPENSADA DE AVCB'
        WHEN prev.id_classif_risco_cor = 10 THEN 'NÍVEL I'
        WHEN prev.id_classif_risco_cor = 11 THEN 'NÍVEL II'
        WHEN prev.id_classif_risco_cor = 12 THEN 'NÍVEL III'
        ELSE '-'
    END AS classif_risco_cor,
	prev.resultado_prevencao_descricao,
	prev.vlr_area,
	prev.classificacao_edificacao_descricao,
	prev.numero_pavimentos,
	CASE 
		WHEN prev.id_tipo_projeto = 1 THEN 'PT'
		WHEN prev.id_tipo_projeto = 2 THEN 'PTS'
		WHEN prev.id_tipo_projeto = NULL THEN '-'
		ELSE '-'
	END AS 'TIPO_PROJETO'
FROM db_bisp_reds_reporting.tb_prevencao_vistoria_ocorrencia prev
WHERE 1=1
	AND YEAR(prev.data_hora_fato) = :ANO_FATO
),

OCORRENCIA AS (
SELECT
	oco.numero_ocorrencia,
	YEAR(oco.data_hora_fato),
	oco.unidade_responsavel_registro_nome,
	oco.natureza_codigo,
	oco.natureza_descricao,
	oco.tipo_logradouro_descricao,
	oco.descricao_endereco,
	oco.numero_endereco,
	oco.nome_bairro,
	oco.codigo_municipio,
	oco.nome_municipio,
	MONTH(oco.data_hora_fato) AS 'MesFato',
	FROM_TIMESTAMP(oco.data_hora_fato, 'dd/MM/yyyy') as 'DataFato',
	FROM_TIMESTAMP(oco.data_hora_fato, 'HH:mm:ss') AS 'HoraFato',
	MONTH(oco.data_hora_inclusao) AS 'MesRegistro',
	FROM_TIMESTAMP(oco.data_hora_fim_preenchimento, 'dd/MM/yyyy') as 'DataRegistro',
	FROM_TIMESTAMP(oco.data_hora_fim_preenchimento, 'HH:mm:ss') AS 'HoraRegistro',
	FROM_TIMESTAMP(oco.data_hora_comunicacao, 'dd/MM/yyyy') as 'DataComunicacao',
	FROM_TIMESTAMP(oco.data_hora_comunicacao, 'HH:mm:ss') AS 'HoraComunicacao',
	FROM_TIMESTAMP(oco.data_hora_local, 'dd/MM/yyyy') as 'DataLocal',
	FROM_TIMESTAMP(oco.data_hora_local, 'HH:mm:ss') AS 'HoraLocal',
	FROM_TIMESTAMP(oco.data_hora_final, 'dd/MM/yyyy') as 'DataFinal',
	FROM_TIMESTAMP(oco.data_hora_final, 'HH:mm:ss') AS 'HoraFinal',
    CASE 
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 1 THEN 'Domingo'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 2 THEN 'Segunda-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 3 THEN 'Terça-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 4 THEN 'Quarta-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 5 THEN 'Quinta-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 6 THEN 'Sexta-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 7 THEN 'Sábado'
	    ELSE '-'
	END AS 'Diasemana',
	oco.relator_matricula, 
	oco.relator_cargo,
	oco.relator_nome,
	oco.relator_nome_unidade,
	oco.data_hora_fato 
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE 1=1
	AND oco.unidade_responsavel_registro_id_orgao = 2
	AND oco.natureza_codigo IN ('P01001', 'P01002', 'P01003', 'P01004', 'P01005', 'P01999', 'P04101', 'P04201', 'X03000', 'X01000', 'W01000')
	AND oco.ind_estado IN ('F', 'R')
	AND oco.unidade_responsavel_registro_nome LIKE '%CAT/%'
	AND YEAR(oco.data_hora_fato) =:ANO_FATO
)

SELECT 
*
FROM OCORRENCIA 
LEFT JOIN PREVENCAO
ON OCORRENCIA.numero_ocorrencia = PREVENCAO.numero_ocorrencia
ORDER BY OCORRENCIA.data_hora_fato ASC
