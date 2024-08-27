WITH 
MUNICIPIOS AS (
SELECT 
	muni.cdmunicipioibge6,
	muni.dsmunicipio,
	CASE 
		WHEN muni.flrmbh = 'N' THEN 'Não'
		WHEN muni.flrmbh = 'S' THEN 'Sim'
		ELSE 'Outros estados'
	END AS RMBH,
	CASE 
		WHEN muni.nmrisp LIKE 'Não se aplica' THEN 'Outros estados'
		ELSE LEFT(muni.nmrisp, INSTR(muni.nmrisp, 'RISP') + 3) 
	END AS RISP 
FROM db_bisp_shared.tb_dim_municipio muni
WHERE 1=1
	AND nrlongitude <> 9999
),


OCORRENCIA AS (
SELECT
	ROW_NUMBER() OVER (ORDER BY oco.data_hora_fato ASC) AS ID_CINDS,
	YEAR(oco.data_hora_fato),
	oco.numero_ocorrencia,
	oco.unidade_responsavel_registro_nome,
	SUBSTRING(oco.natureza_codigo, 1, 1) AS 'Grupo',
	SUBSTRING(oco.natureza_codigo, 1, 3) AS 'Classe',
	oco.natureza_codigo,
	oco.tipo_logradouro_descricao,
	oco.logradouro_nome,
	oco.numero_endereco,
	oco.descricao_complemento_endereco,
	oco.tipo_logradouro2_descricao,
	oco.logradouro2_nome,
	oco.nome_bairro,
	oco.nome_municipio,
	oco.numero_latitude,
	oco.numero_longitude,
	oco.codigo_municipio,
	MONTH(oco.data_hora_fato) AS 'MesFato',
	FROM_TIMESTAMP(oco.data_hora_fato, 'dd/MM/yyyy') as 'DataFato',
	FROM_TIMESTAMP(oco.data_hora_fato, 'HH:mm:ss') AS 'HoraFato',
	FROM_TIMESTAMP(oco.data_hora_fim_preenchimento, 'dd/MM/yyyy') as 'DataRegistro',
	FROM_TIMESTAMP(oco.data_hora_fim_preenchimento, 'HH:mm:ss') AS 'HoraRegistro',
	FROM_TIMESTAMP(oco.data_hora_comunicacao, 'dd/MM/yyyy') as 'DataComunicacao',
	FROM_TIMESTAMP(oco.data_hora_comunicacao, 'HH:mm:ss') AS 'HoraComunicacao',
	FROM_TIMESTAMP(oco.data_hora_local, 'dd/MM/yyyy') as 'DataLocal',
	FROM_TIMESTAMP(oco.data_hora_local, 'HH:mm:ss') AS 'HoraLocal',
	FROM_TIMESTAMP(oco.data_hora_final, 'dd/MM/yyyy') as 'DataFinal',
	FROM_TIMESTAMP(oco.data_hora_final, 'HH:mm:ss') AS 'HoraFinal',
	MONTH(oco.data_hora_fim_preenchimento) AS 'MesRegistro',
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
	oco.descricao_estado,
	oco.relator_matricula,
	oco.relator_cargo ,
	oco.relator_nome,
	oco.relator_nome_unidade,
	oco.id_unid_resp_registro
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE 1=1
	AND YEAR(oco.data_hora_fato) =:ANO_FATO
	AND oco.unidade_responsavel_registro_id_orgao = 2 
	AND oco.natureza_codigo not like 'U30%' 
	AND oco.natureza_codigo not like 'W06%' 
	AND oco.ind_estado IN ('F', 'R')
),

INCEDIO AS (
SELECT 
	ince.numero_ocorrencia,
	ince.qtd_agua_utilizada,
	ince.florestal_qtd_area_queimada
FROM db_bisp_reds_reporting.tb_incendio_ocorrencia ince
WHERE 1=1
	AND YEAR(ince.data_hora_fato) = :ANO_FATO
),


INTEGRANTE AS (
SELECT 
inte.numero_ocorrencia,
COUNT(DISTINCT (inte.numero_matricula)) AS qtde_inte
FROM db_bisp_reds_reporting.tb_integrante_guarnicao_ocorrencia inte
WHERE 1=1
	AND YEAR(inte.data_hora_fato) = :ANO_FATO
GROUP BY 1
)

SELECT
OCORRENCIA.*,
MUNICIPIOS.dsmunicipio,
MUNICIPIOS.RMBH,
MUNICIPIOS.RISP,
INTEGRANTE.qtde_inte,
INCEDIO.florestal_qtd_area_queimada,
INCEDIO.qtd_agua_utilizada
FROM OCORRENCIA
	LEFT JOIN MUNICIPIOS ON OCORRENCIA.codigo_municipio = MUNICIPIOS.cdmunicipioibge6
	LEFT JOIN INTEGRANTE ON OCORRENCIA.numero_ocorrencia = INTEGRANTE.numero_ocorrencia
	LEFT JOIN INCEDIO ON OCORRENCIA.numero_ocorrencia = INCEDIO.numero_ocorrencia
ORDER BY OCORRENCIA.ID_CINDS ASC