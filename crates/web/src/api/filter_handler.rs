use axum::Json;
use data_mover::{FilterFieldDef, get_filter_field_definitions};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct OperatorDto {
    value: &'static str,
    label: &'static str,
}

#[derive(Debug, Serialize)]
pub struct FieldDto {
    name: &'static str,
    label: &'static str,
    value_type: &'static str,
    operators: Vec<OperatorDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    enum_values: Option<Vec<&'static str>>,
}

#[derive(Debug, Serialize)]
pub struct FilterFieldsResponse {
    fields: Vec<FieldDto>,
}

fn to_dto(def: FilterFieldDef) -> FieldDto {
    FieldDto {
        name: def.name,
        label: def.label,
        value_type: def.value_type,
        operators: def
            .operators
            .into_iter()
            .map(|op| OperatorDto {
                value: op.value,
                label: op.label,
            })
            .collect(),
        enum_values: def.enum_values,
    }
}

/// GET /api/v1/filter-fields
///
/// 返回后端支持的过滤条件字段与操作符定义，供前端条件构建器动态渲染。
/// 数据来源: `data_mover::filter::get_filter_field_definitions()`
pub async fn get_filter_fields() -> Json<FilterFieldsResponse> {
    let fields = get_filter_field_definitions().into_iter().map(to_dto).collect();
    Json(FilterFieldsResponse { fields })
}
