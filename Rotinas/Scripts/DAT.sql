WITH 
OCORRENCIA AS (
SELECT 
	oco.unidade_responsavel_registro_nome,
	oco.numero_ocorrencia,
	oco.numero_latitude,
	oco.numero_longitude,
	oco.data_hora_fato,
	FROM_TIMESTAMP(oco.data_hora_fato, 'dd/MM/yyyy') as 'DataFato',
	oco.natureza_descricao,
	oco.natureza_codigo,
	YEAR(oco.data_hora_fato),
	SUBSTRING(oco.natureza_codigo, 1, 3) AS 'Cod.Grupo Natureza'
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE 1=1
	AND YEAR(oco.data_hora_fato) =:ANO_FATO 
	AND oco.ind_estado IN ('F', 'R')
	AND SUBSTRING(oco.natureza_codigo, 1, 3) = 'P01' 
),

VISTORIA AS (
SELECT
	vist.numero_ocorrencia,
	vist.ocupacao_divisao_descricao,
	CASE 
		WHEN vist.id_classif_risco_cor = 9 THEN 'ALTO RISCO'
		WHEN vist.id_classif_risco_cor = 8 THEN 'BAIXO RISCO'
		WHEN vist.id_classif_risco_cor = 7 THEN 'DISPENSADA DE AVCB'
        WHEN vist.id_classif_risco_cor = 10 THEN 'NÍVEL I'
        WHEN vist.id_classif_risco_cor = 11 THEN 'NÍVEL II'
        WHEN vist.id_classif_risco_cor = 12 THEN 'NÍVEL III'
		ELSE '-'
	END AS classif_risco_cor,
	vist.resultado_prevencao_descricao,
	vist.ocupacao_divisao_codigo,
	vist.numero_projeto,
	vist.vlr_area,
	vist.classificacao_edificacao_descricao,
	vist.classificacao_ito21_descricao,
	FROM_TIMESTAMP(vist.data_concessao, 'dd/MM/yyyy') as 'data_concessao',
	vist.numero_pavimentos
FROM db_bisp_reds_reporting.tb_prevencao_vistoria_ocorrencia vist
WHERE 1=1
	AND YEAR(vist.data_hora_fato) = :ANO_FATO
)

SELECT	
*
FROM OCORRENCIA
LEFT JOIN VISTORIA ON VISTORIA.numero_ocorrencia = OCORRENCIA.numero_ocorrencia
ORDER BY OCORRENCIA.data_hora_fato ASC;