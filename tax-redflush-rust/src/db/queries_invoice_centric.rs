use crate::models::{InvoiceCoverage, InvoiceItemDetail};
use sqlx::PgPool;

/// 批量查询发票覆盖度统计
/// 按SKU覆盖数量降序、总金额降序排序
pub async fn query_invoices_with_coverage(
    pool: &PgPool,
    buyer_tax_no: &str,
    seller_tax_no: &str,
    sku_list: &[String],
) -> Result<Vec<InvoiceCoverage>, sqlx::Error> {
    sqlx::query_as::<_, InvoiceCoverage>(
        r#"
        WITH required_skus AS (
            SELECT unnest($1::varchar[]) as fspbm
        ),
        invoice_coverage AS (
            SELECT
                vi.fid as invoice_id,
                COUNT(DISTINCT vii.fspbm) as sku_coverage_count,
                COALESCE(SUM(vii.famount), 0) as total_coverage_amount
            FROM t_sim_vatinvoice_item_1201 vii
            INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
            INNER JOIN required_skus rs ON vii.fspbm = rs.fspbm
            WHERE vi.fbuyertaxno = $2
              AND vi.fsalertaxno = $3
              AND vi.ftotalamount > 0
              AND vii.famount > 0
            GROUP BY vi.fid
        )
        SELECT invoice_id, sku_coverage_count, total_coverage_amount
        FROM invoice_coverage
        ORDER BY sku_coverage_count DESC, total_coverage_amount DESC
        "#,
    )
    .bind(sku_list)
    .bind(buyer_tax_no)
    .bind(seller_tax_no)
    .fetch_all(pool)
    .await
}

/// 批量获取多张发票的明细（仅限指定SKU）
pub async fn query_items_for_invoices(
    pool: &PgPool,
    invoice_ids: &[i64],
    sku_list: &[String],
) -> Result<Vec<InvoiceItemDetail>, sqlx::Error> {
    sqlx::query_as::<_, InvoiceItemDetail>(
        r#"
        SELECT
            vii.fid as invoice_id,
            vii.fentryid as item_id,
            vii.fspbm as product_code,
            vii.fnum as quantity,
            vii.famount as amount,
            vii.funitprice as unit_price
        FROM t_sim_vatinvoice_item_1201 vii
        WHERE vii.fid = ANY($1)
          AND vii.fspbm = ANY($2)
          AND vii.famount > 0
        ORDER BY vii.fid, vii.famount DESC
        "#,
    )
    .bind(invoice_ids)
    .bind(sku_list)
    .fetch_all(pool)
    .await
}

/// 一次性查询所有候选发票明细（用于Invoice-Centric算法）
/// 直接返回所有匹配的发票明细，在内存中处理评分
pub async fn query_all_candidate_items(
    pool: &PgPool,
    buyer_tax_no: &str,
    seller_tax_no: &str,
    sku_list: &[String],
) -> Result<Vec<InvoiceItemDetail>, sqlx::Error> {
    sqlx::query_as::<_, InvoiceItemDetail>(
        r#"
        SELECT
            vii.fid as invoice_id,
            vii.fentryid as item_id,
            vii.fspbm as product_code,
            vii.fnum as quantity,
            vii.famount as amount,
            vii.funitprice as unit_price
        FROM t_sim_vatinvoice_item_1201 vii
        INNER JOIN t_sim_vatinvoice_1201 vi ON vi.fid = vii.fid
        WHERE vii.fspbm = ANY($1)
          AND vi.fbuyertaxno = $2
          AND vi.fsalertaxno = $3
          AND vi.ftotalamount > 0
          AND vii.famount > 0
        ORDER BY vii.fid, vii.famount DESC
        "#,
    )
    .bind(sku_list)
    .bind(buyer_tax_no)
    .bind(seller_tax_no)
    .fetch_all(pool)
    .await
}

/// Phase 1: 仅查询候选发票ID (快速筛选)
pub async fn query_candidate_invoice_ids(
    pool: &PgPool,
    buyer_tax_no: &str,
    seller_tax_no: &str,
) -> Result<Vec<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT fid
        FROM t_sim_vatinvoice_1201
        WHERE fbuyertaxno = $1
          AND fsalertaxno = $2
          AND ftotalamount > 0
        "#,
    )
    .bind(buyer_tax_no)
    .bind(seller_tax_no)
    .fetch_all(pool)
    .await
}

/// Phase 2: 按发票ID列表批量查询明细
pub async fn query_items_by_fids_and_skus(
    pool: &PgPool,
    invoice_ids: &[i64],
    sku_list: &[String],
) -> Result<Vec<InvoiceItemDetail>, sqlx::Error> {
    sqlx::query_as::<_, InvoiceItemDetail>(
        r#"
        SELECT
            vii.fid as invoice_id,
            vii.fentryid as item_id,
            vii.fspbm as product_code,
            vii.fnum as quantity,
            vii.famount as amount,
            vii.funitprice as unit_price
        FROM t_sim_vatinvoice_item_1201 vii
        WHERE vii.fid = ANY($1)
          AND vii.fspbm = ANY($2)
          AND vii.famount > 0
        ORDER BY vii.fid, vii.famount DESC
        "#,
    )
    .bind(invoice_ids)
    .bind(sku_list)
    .fetch_all(pool)
    .await
}
