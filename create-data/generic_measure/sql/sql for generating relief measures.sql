-- 130 Get Registered aircraft relief
select distinct goods_nomenclature_item_id, 'S1900010' as measure_generating_regulation_id,
'1011' as geographical_area_id, '130' as measure_type_id, 0 as ad_valorem, null as specific1,
null as ceiling, null as minimum, null as specific2, '2019-11-01' as validity_start_date, null as validity_end_date
from goods_nomenclatures gn
where gn.producline_suffix = '80'
and gn.validity_end_date is null
and gn.goods_nomenclature_item_id in ('8802110000', '8802120000', '8802200000', '8802300000', '8802400000')
order by goods_nomenclature_item_id

-- 131 pharma
select distinct m.goods_nomenclature_item_id, 'S1900011' as measure_generating_regulation_id,
'1011' as geographical_area_id, '131' as measure_type_id, 0 as ad_valorem, null as specific1,
null as ceiling, null as minimum, null as specific2, '2019-11-01' as validity_start_date, null as validity_end_date
from ml.v5 m, goods_nomenclatures gn
where m.goods_nomenclature_item_id = gn.goods_nomenclature_item_id
and gn.producline_suffix = '80'
and gn.validity_end_date is null and m.validity_end_date is null
and additional_code_type_id = '2' and additional_code_id = '500' 
order by goods_nomenclature_item_id

-- 132 vessels / platform relief
select distinct m.goods_nomenclature_item_id, 'S1900012' as measure_generating_regulation_id,
'1011' as geographical_area_id, '132' as measure_type_id, 0 as ad_valorem, null as specific1,
null as ceiling, null as minimum, null as specific2, '2019-11-01' as validity_start_date, null as validity_end_date
from ml.v5 m, goods_nomenclatures gn
where m.goods_nomenclature_item_id = gn.goods_nomenclature_item_id
and gn.producline_suffix = '80'
and gn.validity_end_date is null
and measure_type_id = '117' and m.validity_end_date is null
order by goods_nomenclature_item_id

-- 133 Authorised release relief
select distinct m.goods_nomenclature_item_id, 'S1900013' as measure_generating_regulation_id, 
'1011' as geographical_area_id, '133' as measure_type_id, 0 as ad_valorem, null as specific1,
null as ceiling, null as minimum, null as specific2, '2019-11-01' as validity_start_date, null as validity_end_date
from ml.v5 m, goods_nomenclatures gn
where m.goods_nomenclature_item_id = gn.goods_nomenclature_item_id
and gn.producline_suffix = '80'
and gn.validity_end_date is null
and measure_type_id = '119' and m.validity_end_date is null
order by goods_nomenclature_item_id

-- 134 certain aircraft relief
select distinct m.goods_nomenclature_item_id, 'S1900014' as measure_generating_regulation_id,
'1011' as geographical_area_id, '134' as measure_type_id, 0 as ad_valorem, null as specific1,
null as ceiling, null as minimum, null as specific2, '2019-11-01' as validity_start_date, null as validity_end_date
from ml.v5 m, goods_nomenclatures gn, goods_nomenclature_descriptions gnd
where m.goods_nomenclature_item_id = gn.goods_nomenclature_item_id
and gn.goods_nomenclature_item_id = gnd.goods_nomenclature_item_id
and gn.producline_suffix = gnd.productline_suffix
and gn.producline_suffix = '80'
and m.measure_type_id = '115'
and lower(gnd.description) like '%certain types of aircraft%'
and gn.validity_end_date is null and m.validity_end_date is null 
order by goods_nomenclature_item_id
