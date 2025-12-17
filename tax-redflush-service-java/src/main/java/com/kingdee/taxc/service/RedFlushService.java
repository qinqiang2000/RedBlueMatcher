package com.kingdee.taxc.service;

import com.kingdee.taxc.dto.MatchRequest;
import com.kingdee.taxc.dto.MatchResult;
import com.kingdee.taxc.dto.MatchedInvoiceItem;
import com.kingdee.taxc.dto.BatchMatchRequest;
import com.kingdee.taxc.mapper.RedFlushMapper;
import com.kingdee.taxc.mapper.MatchBill1201Mapper;
import com.kingdee.taxc.mapper.MatchBillItem1201Mapper;
import com.kingdee.taxc.mapper.MatchResult1201Mapper;
import com.kingdee.taxc.mapper.MatchTempSummary1201Mapper;
import com.kingdee.taxc.entity.MatchBill1201;
import com.kingdee.taxc.entity.MatchResult1201;
import com.kingdee.taxc.entity.MatchTempSummary1201;
import com.kingdee.taxc.dto.CandidateStat;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.Map;
import java.util.HashMap;

@Service
public class RedFlushService {
    private final RedFlushMapper mapper;
    private final MatchBill1201Mapper billMapper;
    private final MatchBillItem1201Mapper billItemMapper;
    private final MatchResult1201Mapper resultMapper;
    private final MatchTempSummary1201Mapper tempSummaryMapper;
    private static final Logger log = LoggerFactory.getLogger(RedFlushService.class);
    private static final Logger progressLog = LoggerFactory.getLogger("com.kingdee.taxc.progress");

    public RedFlushService(RedFlushMapper mapper, MatchBill1201Mapper billMapper, MatchBillItem1201Mapper billItemMapper, MatchResult1201Mapper resultMapper, MatchTempSummary1201Mapper tempSummaryMapper) {
        this.mapper = mapper;
        this.billMapper = billMapper;
        this.billItemMapper = billItemMapper;
        this.resultMapper = resultMapper;
        this.tempSummaryMapper = tempSummaryMapper;
    }

    public MatchResult match(MatchRequest req) {
        List<MatchedInvoiceItem> items;
        if (req.getNegativeApplyId() != null) {
            MatchBill1201 bill = billMapper.selectById(req.getNegativeApplyId());
            List<com.kingdee.taxc.entity.MatchBillItem1201> billItems = billItemMapper.listByBillId(req.getNegativeApplyId());
            for (int idx = 0; idx < billItems.size(); idx++) {
                com.kingdee.taxc.entity.MatchBillItem1201 bi = billItems.get(idx);
                String code = bi.getFspbm();
                BigDecimal target = bi.getFamount();
                BigDecimal targetAbs = target == null ? BigDecimal.ZERO : target.abs();
                log.info("处理单据 {} 商品编码 {} 剩余未处理 {}", req.getNegativeApplyId(), code, billItems.size() - idx - 1);
                List<MatchedInvoiceItem> candidates = mapper.matchByTaxAndProduct(bill.getFbuyertaxno(), bill.getFsalertaxno(), code);
                BigDecimal sum = BigDecimal.ZERO;
                List<MatchResult1201> batch = new ArrayList<>();
                boolean exactMatched = false;
                for (MatchedInvoiceItem mi : candidates) {
                    if (mi.getAmount().compareTo(targetAbs) == 0) {
                        MatchResult1201 rec = new MatchResult1201();
                        rec.setFbillid(req.getNegativeApplyId());
                        rec.setFbuyertaxno(bill.getFbuyertaxno());
                        rec.setFsalertaxno(bill.getFsalertaxno());
                        rec.setFspbm(mi.getProductCode());
                        rec.setFinvoiceid(mi.getInvoiceId());
                        rec.setFinvoiceitemid(mi.getItemId());
                        rec.setFnum(mi.getQuantity());
                        rec.setFbillamount(bi.getFamount());
                        rec.setFinvoiceamount(mi.getAmount());
                        rec.setFmatchamount(targetAbs);
                        rec.setFbillunitprice(bi.getFunitprice());
                        rec.setFbillqty(bi.getFnum());
                        rec.setFinvoiceunitprice(mi.getUnitPrice());
                        rec.setFinvoiceqty(mi.getQuantity());
                        rec.setFbillunitprice(bi.getFunitprice());
                        rec.setFbillqty(bi.getFnum());
                        rec.setFinvoiceunitprice(mi.getUnitPrice());
                        rec.setFinvoiceqty(mi.getQuantity());
                        rec.setFmatchtime(LocalDateTime.now());
                        batch.add(rec);
                        exactMatched = true;
                        break;
                    }
                }
                if (!exactMatched) {
                    for (MatchedInvoiceItem mi : candidates) {
                        BigDecimal remaining = targetAbs.subtract(sum);
                        if (mi.getAmount().compareTo(remaining) <= 0) {
                            sum = sum.add(mi.getAmount());
                            MatchResult1201 rec = new MatchResult1201();
                            rec.setFbillid(req.getNegativeApplyId());
                            rec.setFbuyertaxno(bill.getFbuyertaxno());
                            rec.setFsalertaxno(bill.getFsalertaxno());
                            rec.setFspbm(mi.getProductCode());
                            rec.setFinvoiceid(mi.getInvoiceId());
                            rec.setFinvoiceitemid(mi.getItemId());
                            rec.setFnum(mi.getQuantity());
                            rec.setFbillamount(bi.getFamount());
                            rec.setFinvoiceamount(mi.getAmount());
                            rec.setFmatchamount(mi.getAmount());
                            rec.setFbillunitprice(bi.getFunitprice());
                            rec.setFbillqty(bi.getFnum());
                            rec.setFinvoiceunitprice(mi.getUnitPrice());
                            rec.setFinvoiceqty(mi.getQuantity());
                            rec.setFbillunitprice(bi.getFunitprice());
                            rec.setFbillqty(bi.getFnum());
                            rec.setFinvoiceunitprice(mi.getUnitPrice());
                            rec.setFinvoiceqty(mi.getQuantity());
                            rec.setFmatchtime(LocalDateTime.now());
                            batch.add(rec);
                            if (sum.compareTo(targetAbs) == 0) {
                                break;
                            }
                        } else {
                            MatchResult1201 rec = new MatchResult1201();
                            rec.setFbillid(req.getNegativeApplyId());
                            rec.setFbuyertaxno(bill.getFbuyertaxno());
                            rec.setFsalertaxno(bill.getFsalertaxno());
                            rec.setFspbm(mi.getProductCode());
                            rec.setFinvoiceid(mi.getInvoiceId());
                            rec.setFinvoiceitemid(mi.getItemId());
                            rec.setFnum(mi.getQuantity());
                            rec.setFbillamount(bi.getFamount());
                            rec.setFinvoiceamount(mi.getAmount());
                            rec.setFmatchamount(remaining);
                            rec.setFbillunitprice(bi.getFunitprice());
                            rec.setFbillqty(bi.getFnum());
                            rec.setFinvoiceunitprice(mi.getUnitPrice());
                            rec.setFinvoiceqty(mi.getQuantity());
                            rec.setFbillunitprice(bi.getFunitprice());
                            rec.setFbillqty(bi.getFnum());
                            rec.setFinvoiceunitprice(mi.getUnitPrice());
                            rec.setFinvoiceqty(mi.getQuantity());
                            rec.setFmatchtime(LocalDateTime.now());
                            batch.add(rec);
                            break;
                        }
                    }
                }
                if (!batch.isEmpty()) {
                    insertBatchChunked(batch);
                }
            }
            MatchResult r = new MatchResult();
            r.setBuyerTaxNo(bill.getFbuyertaxno());
            r.setSellerTaxNo(bill.getFsalertaxno());
            r.setItems(new ArrayList<>());
            return r;
        } else {
            items = mapper.matchByTaxAndProduct(req.getBuyerTaxNo(), req.getSellerTaxNo(), req.getProductCode());
        }
        MatchResult r = new MatchResult();
        r.setBuyerTaxNo(req.getBuyerTaxNo());
        r.setSellerTaxNo(req.getSellerTaxNo());
        r.setProductCode(req.getProductCode());
        r.setItems(items);
        return r;
    }

    public List<MatchResult> batchMatch(BatchMatchRequest req) {
        List<MatchResult> results = new ArrayList<>();
        if (req.getBillIds() == null || req.getBillIds().isEmpty()) {
            return results;
        }
        for (Long billId : req.getBillIds()) {
            MatchBill1201 bill = billMapper.selectById(billId);
            if (bill == null) {
                continue;
            }
            List<com.kingdee.taxc.entity.MatchBillItem1201> billItems = billItemMapper.listByBillId(billId);
            for (int idx = 0; idx < billItems.size(); idx++) {
                com.kingdee.taxc.entity.MatchBillItem1201 bi = billItems.get(idx);
                String code = bi.getFspbm();
                BigDecimal target = bi.getFamount();
                BigDecimal targetAbs = target == null ? BigDecimal.ZERO : target.abs();
                log.info("处理单据 {} 商品编码 {} 剩余未处理 {}", billId, code, billItems.size() - idx - 1);
                List<MatchedInvoiceItem> candidates = mapper.matchByTaxAndProduct(bill.getFbuyertaxno(), bill.getFsalertaxno(), code);
                BigDecimal sum = BigDecimal.ZERO;
                List<MatchResult1201> batch = new ArrayList<>();
                boolean exactMatched = false;
                for (MatchedInvoiceItem mi : candidates) {
                    if (mi.getAmount().compareTo(targetAbs) == 0) {
                        MatchResult1201 rec = new MatchResult1201();
                        rec.setFbillid(billId);
                        rec.setFbuyertaxno(bill.getFbuyertaxno());
                        rec.setFsalertaxno(bill.getFsalertaxno());
                        rec.setFspbm(mi.getProductCode());
                        rec.setFinvoiceid(mi.getInvoiceId());
                        rec.setFinvoiceitemid(mi.getItemId());
                        rec.setFnum(mi.getQuantity());
                        rec.setFbillamount(bi.getFamount());
                        rec.setFinvoiceamount(mi.getAmount());
                        rec.setFmatchamount(targetAbs);
                        rec.setFmatchtime(LocalDateTime.now());
                        batch.add(rec);
                        exactMatched = true;
                        break;
                    }
                }
                if (!exactMatched) {
                    for (MatchedInvoiceItem mi : candidates) {
                        BigDecimal remaining = targetAbs.subtract(sum);
                        if (mi.getAmount().compareTo(remaining) <= 0) {
                            sum = sum.add(mi.getAmount());
                            MatchResult1201 rec = new MatchResult1201();
                            rec.setFbillid(billId);
                            rec.setFbuyertaxno(bill.getFbuyertaxno());
                            rec.setFsalertaxno(bill.getFsalertaxno());
                            rec.setFspbm(mi.getProductCode());
                            rec.setFinvoiceid(mi.getInvoiceId());
                            rec.setFinvoiceitemid(mi.getItemId());
                            rec.setFnum(mi.getQuantity());
                            rec.setFbillamount(bi.getFamount());
                            rec.setFinvoiceamount(mi.getAmount());
                            rec.setFmatchamount(mi.getAmount());
                            rec.setFmatchtime(LocalDateTime.now());
                            batch.add(rec);
                            if (sum.compareTo(targetAbs) == 0) {
                                break;
                            }
                        } else {
                            MatchResult1201 rec = new MatchResult1201();
                            rec.setFbillid(billId);
                            rec.setFbuyertaxno(bill.getFbuyertaxno());
                            rec.setFsalertaxno(bill.getFsalertaxno());
                            rec.setFspbm(mi.getProductCode());
                            rec.setFinvoiceid(mi.getInvoiceId());
                            rec.setFinvoiceitemid(mi.getItemId());
                            rec.setFnum(mi.getQuantity());
                            rec.setFbillamount(bi.getFamount());
                            rec.setFinvoiceamount(mi.getAmount());
                            rec.setFmatchamount(remaining);
                            rec.setFmatchtime(LocalDateTime.now());
                            batch.add(rec);
                            break;
                        }
                    }
                }
                if (!batch.isEmpty()) {
                    insertBatchChunked(batch);
                }
            }
            MatchResult r = new MatchResult();
            r.setBuyerTaxNo(bill.getFbuyertaxno());
            r.setSellerTaxNo(bill.getFsalertaxno());
            r.setItems(new ArrayList<>());
            results.add(r);
        }
        return results;
    }

    // 批量临时策略匹配：按数据库排序的商品顺序，优先复用已匹配发票，再顺序填充到目标金额
    public void batchMatchTempStrategy(BatchMatchRequest req) {
        if (req.getBillIds() == null || req.getBillIds().isEmpty()) {
            return;
        }
        for (Long billId : req.getBillIds()) {
            MatchBill1201 bill = billMapper.selectById(billId);
            if (bill == null) {
                continue;
            }
            List<com.kingdee.taxc.entity.MatchBillItem1201> billItems = billItemMapper.listByBillId(billId);
            if (billItems.isEmpty()) {
                continue;
            }
            List<MatchTempSummary1201> summaries = new ArrayList<>();
            for (int idx = 0; idx < billItems.size(); idx++) {
                com.kingdee.taxc.entity.MatchBillItem1201 bi = billItems.get(idx);
                log.info("统计单据 {} 商品编码 {} 剩余未处理 {}", billId, bi.getFspbm(), billItems.size() - idx - 1);
                CandidateStat stat = mapper.statForProduct(bill.getFbuyertaxno(), bill.getFsalertaxno(), bi.getFspbm());
                MatchTempSummary1201 s = new MatchTempSummary1201();
                s.setJobid(billId);
                s.setFspbm(bi.getFspbm());
                s.setItemCount(stat == null ? 0L : stat.getCnt());
                s.setTotalAmount(stat == null ? java.math.BigDecimal.ZERO : stat.getSumAmount());
                summaries.add(s);
            }
            tempSummaryMapper.clearByJob(billId);
            insertTempSummaryChunked(summaries);
            // 从数据库获取排序好的商品汇总（按明细行数、总金额升序）
            List<MatchTempSummary1201> ordered = tempSummaryMapper.listByJobOrdered(billId);
            List<com.kingdee.taxc.entity.MatchBillItem1201> orderedItems = new ArrayList<>();
            for (MatchTempSummary1201 s : ordered) {
                for (com.kingdee.taxc.entity.MatchBillItem1201 bi : billItems) {
                    if (s.getFspbm().equals(bi.getFspbm())) {
                        orderedItems.add(bi);
                    }
                }
            }
            billItems = orderedItems;

            // 已匹配过的发票集合（用于优先复用）；每个商品的累计已匹配金额
            java.util.LinkedHashSet<Long> preferredInvoices = new java.util.LinkedHashSet<>();
            Map<String, BigDecimal> matchedByProduct = new HashMap<>();

            // ⭐ 新增：进度统计和日志
            int totalSkus = billItems.size();
            int matchedCount = 0;
            log.info("跳过稀缺度预统计，直接开始按需匹配...");
            log.info("处理销购方组: {} 个SKU", totalSkus);

            int idx=0;
            for (com.kingdee.taxc.entity.MatchBillItem1201 bi : billItems) {
                String code = bi.getFspbm();
                java.math.BigDecimal targetAbs = (bi.getFamount() == null ? java.math.BigDecimal.ZERO : bi.getFamount().abs());
                java.math.BigDecimal already = matchedByProduct.getOrDefault(code, java.math.BigDecimal.ZERO);
                java.math.BigDecimal remainingTarget = targetAbs.subtract(already);
                List<MatchResult1201> batch = new ArrayList<>();
                log.debug("匹配单据 {} 商品编码 {} 剩余未处理 {}", billId, bi.getFspbm(), billItems.size() - idx - 1);
                if (remainingTarget.compareTo(java.math.BigDecimal.ZERO) <= 0) {
                    log.debug("已匹配足额 单据 {} 商品编码 {} 已匹配 {} 跳过", billId, code, already);
                    matchedCount++;  // ⭐ 新增：跳过时计数
                    idx++;
                    continue;
                }
                // 构建候选集合：已匹配发票中的该商品 + 常规候选，按 itemId 去重，保证优先复用顺序
                List<MatchedInvoiceItem> source = new ArrayList<>();
                java.util.LinkedHashSet<Long> seenItemIds = new java.util.LinkedHashSet<>();
                if (!preferredInvoices.isEmpty()) {
                    List<Long> ids = new ArrayList<>(preferredInvoices);
                    for (int i = 0; i < ids.size(); i += 1000) {
                        List<Long> subIds = ids.subList(i, Math.min(i + 1000, ids.size()));
                        List<MatchedInvoiceItem> pref = mapper.matchOnInvoices(bill.getFbuyertaxno(), bill.getFsalertaxno(), code, subIds);
                        for (MatchedInvoiceItem mi : pref) {
                            if (seenItemIds.add(mi.getItemId())) {source.add(mi);}
                        }
                    }
                }
                List<MatchedInvoiceItem> general = mapper.matchByTaxAndProduct(bill.getFbuyertaxno(), bill.getFsalertaxno(), code);
                for (MatchedInvoiceItem mi : general) {
                    if (seenItemIds.add(mi.getItemId())){ source.add(mi);}
                }

                // 顺序遍历候选直到填满或候选用尽：每条使用 min(候选金额, 剩余目标)
                java.math.BigDecimal remaining = targetAbs.subtract(matchedByProduct.getOrDefault(code, java.math.BigDecimal.ZERO));
                for (MatchedInvoiceItem mi : source) {
                    if (remaining.compareTo(java.math.BigDecimal.ZERO) <= 0) break;
                    java.math.BigDecimal use = mi.getAmount().compareTo(remaining) >= 0 ? remaining : mi.getAmount();
                    if (use.compareTo(java.math.BigDecimal.ZERO) <= 0) continue;
                    MatchResult1201 rec = new MatchResult1201();
                    rec.setFbillid(billId);
                    rec.setFbuyertaxno(bill.getFbuyertaxno());
                    rec.setFsalertaxno(bill.getFsalertaxno());
                    rec.setFspbm(mi.getProductCode());
                    rec.setFinvoiceid(mi.getInvoiceId());
                    rec.setFinvoiceitemid(mi.getItemId());
                    rec.setFnum(mi.getQuantity());
                    rec.setFbillamount(bi.getFamount());
                    rec.setFinvoiceamount(mi.getAmount());
                    rec.setFmatchamount(use);
                    rec.setFbillunitprice(bi.getFunitprice());
                    rec.setFbillqty(bi.getFnum());
                    rec.setFinvoiceunitprice(mi.getUnitPrice());
                    rec.setFinvoiceqty(mi.getQuantity());
                    rec.setFmatchtime(LocalDateTime.now());
                    batch.add(rec);
                    preferredInvoices.add(mi.getInvoiceId());
                    matchedByProduct.put(code, matchedByProduct.getOrDefault(code, java.math.BigDecimal.ZERO).add(use));
                    remaining = remaining.subtract(use);
                }
                // 分块落库前计算汇总插入金额，记录进度日志
                java.math.BigDecimal inserted = sumMatchAmount(batch);
                log.debug("汇总插入 单据 {} 商品编码 {} 插入金额 {} 已匹配累计 {} 剩余 {}", billId, code, inserted, matchedByProduct.getOrDefault(code, java.math.BigDecimal.ZERO), targetAbs.subtract(matchedByProduct.getOrDefault(code, java.math.BigDecimal.ZERO)));
                if (!batch.isEmpty()) {
                    insertBatchChunked(batch);
                    matchedCount++;  // ⭐ 新增：匹配成功时计数
                }
                idx++;

                // ⭐ 新增：每100个SKU或第一个SKU输出进度
                if (idx % 100 == 0 || idx == 1) {
                    String progressMsg = String.format("SKU进度: %d/%d, 已匹配: %d, 已用发票: %d",
                                                       idx, totalSkus, matchedCount, preferredInvoices.size());
                    progressLog.info(progressMsg);
                    System.out.println(progressMsg);  // 同时输出到控制台
                }
            }

            // ⭐ 新增：循环结束后输出最终统计
            log.info("匹配完成: 总SKU: {}, 已匹配: {}, 已用发票: {}",
                     totalSkus, matchedCount, preferredInvoices.size());
        }
    }

    private List<MatchResult1201> applyMatchGreedy(Long billId, MatchBill1201 bill, com.kingdee.taxc.entity.MatchBillItem1201 bi, java.math.BigDecimal targetAbs, List<MatchedInvoiceItem> candidates) {
        List<MatchResult1201> out = new ArrayList<>();
        java.math.BigDecimal sum = java.math.BigDecimal.ZERO;
        boolean exactMatched = false;
        for (MatchedInvoiceItem mi : candidates) {
            if (mi.getAmount().compareTo(targetAbs) == 0) {
                MatchResult1201 rec = new MatchResult1201();
                rec.setFbillid(billId);
                rec.setFbuyertaxno(bill.getFbuyertaxno());
                rec.setFsalertaxno(bill.getFsalertaxno());
                rec.setFspbm(mi.getProductCode());
                rec.setFinvoiceid(mi.getInvoiceId());
                rec.setFinvoiceitemid(mi.getItemId());
                rec.setFnum(mi.getQuantity());
                rec.setFbillamount(bi.getFamount());
                rec.setFinvoiceamount(mi.getAmount());
                rec.setFmatchamount(targetAbs);
                rec.setFmatchtime(LocalDateTime.now());
                out.add(rec);
                exactMatched = true;
                break;
            }
        }
        if (exactMatched) return out;
        for (MatchedInvoiceItem mi : candidates) {
            java.math.BigDecimal remaining = targetAbs.subtract(sum);
            if (mi.getAmount().compareTo(remaining) <= 0) {
                sum = sum.add(mi.getAmount());
                MatchResult1201 rec = new MatchResult1201();
                rec.setFbillid(billId);
                rec.setFbuyertaxno(bill.getFbuyertaxno());
                rec.setFsalertaxno(bill.getFsalertaxno());
                rec.setFspbm(mi.getProductCode());
                rec.setFinvoiceid(mi.getInvoiceId());
                rec.setFinvoiceitemid(mi.getItemId());
                rec.setFnum(mi.getQuantity());
                rec.setFbillamount(bi.getFamount());
                rec.setFinvoiceamount(mi.getAmount());
                rec.setFmatchamount(mi.getAmount());
                rec.setFmatchtime(LocalDateTime.now());
                out.add(rec);
                if (sum.compareTo(targetAbs) == 0) break;
            } else {
                MatchResult1201 rec = new MatchResult1201();
                rec.setFbillid(billId);
                rec.setFbuyertaxno(bill.getFbuyertaxno());
                rec.setFsalertaxno(bill.getFsalertaxno());
                rec.setFspbm(mi.getProductCode());
                rec.setFinvoiceid(mi.getInvoiceId());
                rec.setFinvoiceitemid(mi.getItemId());
                rec.setFnum(mi.getQuantity());
                rec.setFbillamount(bi.getFamount());
                rec.setFinvoiceamount(mi.getAmount());
                rec.setFmatchamount(remaining);
                rec.setFmatchtime(LocalDateTime.now());
                out.add(rec);
                break;
            }
        }
        return out;
    }

    private void insertTempSummaryChunked(List<MatchTempSummary1201> list) {
        if (list == null || list.isEmpty()) return;
        int chunk = 1000;
        for (int i = 0; i < list.size(); i += chunk) {
            List<MatchTempSummary1201> sub = list.subList(i, Math.min(i + chunk, list.size()));
            tempSummaryMapper.insertBatch(sub);
        }
    }

    private void insertBatchChunked(List<MatchResult1201> list) {
        if (list == null || list.isEmpty()) {
            return;
        }
        int chunk = 1000;
        for (int i = 0; i < list.size(); i += chunk) {
            List<MatchResult1201> sub = list.subList(i, Math.min(i + chunk, list.size()));
            resultMapper.insertBatch(sub);
        }
    }

    private List<MatchResult1201> capToRemaining(List<MatchResult1201> records, java.math.BigDecimal remaining) {
        if (records == null || records.isEmpty()) return records;
        java.math.BigDecimal total = java.math.BigDecimal.ZERO;
        for (MatchResult1201 r : records) {
            total = total.add(r.getFmatchamount());
        }
        if (total.compareTo(remaining) <= 0) return records;
        java.math.BigDecimal overflow = total.subtract(remaining);
        // Reduce from tail until overflow is absorbed
        for (int i = records.size() - 1; i >= 0 && overflow.compareTo(java.math.BigDecimal.ZERO) > 0; i--) {
            MatchResult1201 rec = records.get(i);
            java.math.BigDecimal amt = rec.getFmatchamount();
            if (amt.compareTo(overflow) > 0) {
                rec.setFmatchamount(amt.subtract(overflow));
                overflow = java.math.BigDecimal.ZERO;
            } else {
                overflow = overflow.subtract(amt);
                records.remove(i);
            }
        }
        return records;
    }

    private java.math.BigDecimal sumMatchAmount(List<MatchResult1201> records) {
        if (records == null || records.isEmpty()) return java.math.BigDecimal.ZERO;
        java.math.BigDecimal total = java.math.BigDecimal.ZERO;
        for (MatchResult1201 r : records) {
            total = total.add(r.getFmatchamount());
        }
        return total;
    }

    private List<MatchResult1201> fillWithCandidates(Long billId, MatchBill1201 bill, com.kingdee.taxc.entity.MatchBillItem1201 bi, java.math.BigDecimal remaining, List<MatchedInvoiceItem> candidates, int startIdx) {
        if (remaining.compareTo(java.math.BigDecimal.ZERO) <= 0 || startIdx >= candidates.size()) return java.util.Collections.emptyList();
        List<MatchedInvoiceItem> sub = candidates.subList(startIdx, candidates.size());
        List<MatchResult1201> res = applyMatchGreedy(billId, bill, bi, remaining, sub);
        return res;
    }
}
