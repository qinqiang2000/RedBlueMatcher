-- DROP TABLE public.t_sim_match_result_1201;

CREATE TABLE public.t_sim_match_result_1201 (
    fid int8 GENERATED ALWAYS AS IDENTITY,
    fbillid int8 NULL,
    fbuyertaxno varchar(50) NOT NULL DEFAULT ' '::character varying,
    fsalertaxno varchar(50) NOT NULL DEFAULT ' '::character varying,
    fspbm varchar(50) NOT NULL DEFAULT ' '::character varying,
    finvoiceid int8 NOT NULL DEFAULT 0,
    finvoiceitemid int8 NOT NULL DEFAULT 0,
    fnum numeric(36,23) NOT NULL DEFAULT 0,
    fbillamount numeric(23,10) NOT NULL DEFAULT 0,
    finvoiceamount numeric(23,10) NOT NULL DEFAULT 0,
    fmatchamount numeric(23,10) NOT NULL DEFAULT 0,
    fbillunitprice numeric(36,23) NOT NULL DEFAULT 0,
    fbillqty numeric(36,23) NOT NULL DEFAULT 0,
    finvoiceunitprice numeric(36,23) NOT NULL DEFAULT 0,
    finvoiceqty numeric(36,23) NOT NULL DEFAULT 0,
    fmatchtime timestamp NULL,
    CONSTRAINT t_sim_match_result_1201_pkey PRIMARY KEY (fid)
);
CREATE INDEX t_sim_match_result_1201_fbillid_idx ON public.t_sim_match_result_1201 USING btree (fbillid);
CREATE INDEX t_sim_match_result_1201_tax_idx ON public.t_sim_match_result_1201 USING btree (fbuyertaxno, fsalertaxno);
CREATE INDEX t_sim_match_result_1201_code_idx ON public.t_sim_match_result_1201 USING btree (fspbm);
CREATE INDEX t_sim_match_result_1201_invoice_idx ON public.t_sim_match_result_1201 USING btree (finvoiceid, finvoiceitemid);
