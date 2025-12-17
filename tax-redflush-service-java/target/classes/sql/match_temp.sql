-- DROP TABLE public.t_sim_match_temp_summary_1201;

CREATE TABLE public.t_sim_match_temp_summary_1201 (
    jobid int8 NOT NULL,
    fspbm varchar(50) NOT NULL,
    item_count int8 NOT NULL DEFAULT 0,
    total_amount numeric(23,10) NOT NULL DEFAULT 0
);
CREATE INDEX t_sim_match_temp_summary_1201_job_fspbm_idx ON public.t_sim_match_temp_summary_1201 USING btree (jobid, fspbm);
