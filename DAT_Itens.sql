WITH 
PREVENCAO AS ( 
SELECT DISTINCT
	prev.numero_ocorrencia,
	prev.id_prevencao_vistoria,
	prev.resultado_prevencao_descricao
FROM db_bisp_reds_reporting.tb_prevencao_vistoria_ocorrencia prev
WHERE 1=1
	AND YEAR(prev.data_hora_fato) =:ANO_FATO
), 

OCORRENCIA AS (
SELECT 
	oco.numero_ocorrencia,
	oco.unidade_responsavel_registro_nome,
	oco.natureza_codigo,
	oco.natureza_descricao,
	oco.nome_municipio,
	FROM_TIMESTAMP(oco.data_hora_fato, 'dd/MM/yyyy') as 'DataFato',
	FROM_TIMESTAMP(oco.data_hora_fato, 'HH:mm:ss') as 'HoraFato'
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE 1=1
	AND oco.natureza_codigo IN ('P01001', 'P01002', 'P01003')
	AND YEAR(oco.data_hora_fato) = :ANO_FATO
	AND oco.ind_estado IN ('F', 'R')
),

LOCAL_VISTORIA AS (
SELECT DISTINCT 
	vist_local.numero_ocorrencia,
	vist_local.descricao_local_irregularidade  
FROM db_bisp_reds_reporting.tb_vistoria_local_irregularidade_ocorrencia vist_local
WHERE 1=1
	AND vist_local.descricao_local_irregularidade IS NOT NULL
	AND YEAR(vist_local.data_hora_fato) = :ANO_FATO
),

ITEM_VISTORIA AS ( 
SELECT DISTINCT
	item.id_prevencao_vistoria, 
	item.item_vistoria_grupo_descricao,
	item.item_vistoria_descricao
FROM db_bisp_reds_reporting.tb_item_vistoriado_ocorrencia item
WHERE 1=1 
	AND item.item_vistoria_descricao NOT LIKE 'NÃO SE APLICA'
	AND YEAR(item.data_hora_fato) = :ANO_FATO
	
)

SELECT 
	PREVENCAO.numero_ocorrencia,
	OCORRENCIA.natureza_codigo,
	OCORRENCIA.natureza_descricao,
	OCORRENCIA.DataFato,
	OCORRENCIA.HoraFato,
	PREVENCAO.resultado_prevencao_descricao,
	OCORRENCIA.unidade_responsavel_registro_nome,
	OCORRENCIA.nome_municipio,
	LOCAL_VISTORIA.descricao_local_irregularidade,
	ITEM_VISTORIA.item_vistoria_grupo_descricao,
	ITEM_VISTORIA.item_vistoria_descricao
FROM OCORRENCIA 
LEFT JOIN PREVENCAO ON OCORRENCIA.numero_ocorrencia = PREVENCAO.numero_ocorrencia
LEFT JOIN LOCAL_VISTORIA ON PREVENCAO.numero_ocorrencia = LOCAL_VISTORIA.numero_ocorrencia
LEFT JOIN ITEM_VISTORIA ON PREVENCAO.id_prevencao_vistoria = ITEM_VISTORIA.id_prevencao_vistoria
WHERE 1=1
    AND ITEM_VISTORIA.item_vistoria_grupo_descricao IS NOT NULL
    AND ITEM_VISTORIA.item_vistoria_descricao IS NOT NULL