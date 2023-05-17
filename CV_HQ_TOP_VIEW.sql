with
    po as (
        select
            sysid,
            mandt,
            ebeln,
            ebelp,
            zterm_po,
            zbd1t_po,
            zbd2t_po,
            zbd3t_po,
            zbd1p_po,
            zbd2p_po,
            zterm_md_po,
            zbd1t_md_po,
            zbd2t_md_po,
            zbd3t_md_po,
            zbd1p_md_po,
            zbd2p_md_po,
            awkey,
            fixed_day_md
        from {{ ref('CV_HQ_VIEW_PO') }}
    -- WHERE (SYSID IN ($IP_SYSID) OR '*' IN ($IP_SYSID))
    ),
    inv as (
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
        from {{ ref('CV_HQ_VIEW_INV') }}
    -- WHERE (SYSID IN ($IP_SYSID) OR '*' IN ($IP_SYSID))
    ),
    pur_cases as (
        select act_run_id, sysid, mandt, ebeln, ebelp
        from {{ source('SAMPLE_POC', 'T_CASES') }}
    -- WHERE (SYSID IN ($IP_SYSID) OR '*' IN ($IP_SYSID))
    ),
    join_1 as (
        select
            po.sysid as sysid,
            po.mandt as mandt,
            po.ebeln as ebeln,
            po.ebelp as ebelp,
            po.zterm_po as zterm_po,
            po.zbd1t_po as zbd1t_po,
            po.zbd2t_po as zbd2t_po,
            po.zbd3t_po as zbd3t_po,
            po.zbd1p_po as zbd1p_po,
            po.zbd2p_po as zbd2p_po,
            po.zterm_md_po as zterm_md_po,
            po.zbd1t_md_po as zbd1t_md_po,
            po.zbd2t_md_po as zbd2t_md_po,
            po.zbd3t_md_po as zbd3t_md_po,
            po.zbd1p_md_po as zbd1p_md_po,
            po.zbd2p_md_po as zbd2p_md_po,
            po.awkey as awkey,
            po.fixed_day_md as fixed_day_md,
            pur_cases.act_run_id as act_run_id
        from po
        inner join
            pur_cases
            on po.sysid = pur_cases.sysid
            and po.mandt = pur_cases.mandt
            and po.ebeln = pur_cases.ebeln
            and po.ebelp = pur_cases.ebelp
    ),
    purchasing as (
        select
            join_1.sysid as sysid,
            join_1.mandt as mandt,
            join_1.ebeln as ebeln,
            join_1.ebelp as ebelp,
            join_1.zterm_po as zterm_po,
            join_1.zbd1t_po as zbd1t_po,
            join_1.zbd2t_po as zbd2t_po,
            join_1.zbd3t_po as zbd3t_po,
            join_1.zbd1p_po as zbd1p_po,
            join_1.zbd2p_po as zbd2p_po,
            join_1.zterm_md_po as zterm_md_po,
            join_1.zbd1t_md_po as zbd1t_md_po,
            join_1.zbd2t_md_po as zbd2t_md_po,
            join_1.zbd3t_md_po as zbd3t_md_po,
            join_1.zbd1p_md_po as zbd1p_md_po,
            join_1.zbd2p_md_po as zbd2p_md_po,
            join_1.awkey as awkey,
            join_1.act_run_id as act_run_id,
            join_1.fixed_day_md as fixed_day_md,
            inv.zterm_inv as zterm_inv,
            inv.zbd1t_inv as zbd1t_inv,
            inv.zbd2t_inv as zbd2t_inv,
            inv.zbd3t_inv as zbd3t_inv,
            inv.zbd1p_inv as zbd1p_inv,
            inv.zbd2p_inv as zbd2p_inv
        from join_1
        left outer join
            inv
            on join_1.sysid = inv.sysid
            and join_1.mandt = inv.mandt
            and join_1.awkey = inv.awkey
    ), filter as (
        select
            zbd2p_inv,
            zbd1p_inv,
            zbd3t_inv,
            zbd2t_inv,
            zbd1t_inv,
            zterm_inv,
            zbd2p_md_po,
            zbd1p_md_po,
            zbd3t_md_po,
            zbd2t_md_po,
            zbd1t_md_po,
            zterm_md_po,
            zbd2p_po,
            zbd1p_po,
            zbd3t_po,
            zbd2t_po,
            zbd1t_po,
            zterm_po,
            ebelp,
            ebeln,
            mandt,
            sysid,
            act_run_id,
            fixed_day_md
        from purchasing
    ),
    pterm as (
        select
            zbd2p_inv,
            zbd1p_inv,
            zbd3t_inv,
            zbd2t_inv,
            zbd1t_inv,
            zterm_inv,
            zbd2p_md_po,
            zbd1p_md_po,
            zbd3t_md_po,
            zbd2t_md_po,
            zbd1t_md_po,
            zterm_md_po,
            zbd2p_po,
            zbd1p_po,
            zbd3t_po,
            zbd2t_po,
            zbd1t_po,
            zterm_po,
            ebelp,
            ebeln,
            mandt,
            sysid,
            act_run_id,
            fixed_day_md,
            left(
                cast(
                    zterm_po
                    || zbd1t_po
                    || zbd2t_po
                    || zbd3t_po
                    || zbd1p_po
                    || zbd2p_po as nvarchar
                ),
                23
            ) as pterm_po,
            left(
                cast(
                    zterm_inv
                    || zbd1t_inv
                    || zbd2t_inv
                    || zbd3t_inv
                    || zbd1p_inv
                    || zbd2p_inv as nvarchar
                ),
                23
            ) as pterm_inv,
            left(
                cast(
                    zterm_md_po
                    || zbd1t_md_po
                    || zbd2t_md_po
                    || zbd3t_md_po
                    || zbd1p_md_po
                    || zbd2p_md_po as nvarchar
                ),
                23
            ) as pterm_md_po
        from filter
    )
select
    act_run_id,
    sysid,
    mandt,
    ebeln,
    ebelp,
    zterm_po,
    zbd1t_po,
    zbd2t_po,
    zbd3t_po,
    zbd1p_po,
    zbd2p_po,
    zterm_md_po,
    zbd1t_md_po,
    zbd2t_md_po,
    zbd3t_md_po,
    zbd1p_md_po,
    zbd2p_md_po,
    zterm_inv,
    zbd1t_inv,
    zbd2t_inv,
    zbd3t_inv,
    zbd1p_inv,
    zbd2p_inv,
    pterm_po,
    pterm_inv,
    pterm_md_po,
    fixed_day_md
from pterm