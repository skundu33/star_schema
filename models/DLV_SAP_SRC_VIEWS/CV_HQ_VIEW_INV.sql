with
    projection_1 as (
        select
            sysid,
            mandt,
            bukrs,
            belnr,
            gjahr,
            buzei,
            zterm_inv,
            zbd1t_inv,
            zbd2t_inv,
            zbd3t_inv,
            zbd1p_inv,
            zbd2p_inv,
            zterm_md_inv,
            zbd1t_md_inv,
            zbd2t_md_inv,
            zbd3t_md_inv,
            zbd1p_md_inv,
            zbd2p_md_inv,
            awkey
        from {{ source('SAMPLE_POC', 'T_CV_HQ_VIEW_INV') }}
    )
select * from projection_1
