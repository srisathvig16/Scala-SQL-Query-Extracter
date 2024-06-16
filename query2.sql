           .select($"PRCHSR_ORG_NBR",
                   $"RLTD_PRCHSR_ORG_NBR",
                   $"prod_id",
                   $"BNFT_PKG_ID",
                   $"PKG_NBR",
                   $"MBRSHP_SOR_CD",
                   $"RLTD_PRCHSR_ORG_TYPE_CD",
                   $"PROD_SOR_CD",
                   $"ELGBLTY_CLNDR_MNTH_END_DT",
                   $"MBR_KEY",
                   $"BNFT_PKG_KEY",
                   $"MBR_PROD_ENRLMNT_EFCTV_DT",
				   $"MAX_FUNDG_CF_CD"
                   ,$"FUNDG_CF_CD"
				   ,$"src_grp_nbr"
				   ,$"mdcl_mbr_cvrg_cnt"
				   ,$"vsn_mbr_cnt"
				   ,$"dntl_mbr_cnt"
				   ,$"mbr_prod_enrlmnt_trmntn_dt"
				   ,$"cntrct_type_cd"
				   ,$"othr_insrnc_type_cd"
				   ,$"gndr_cd"
				   ,$"mbr_mnth_cob_cd"
				   ,$"prcp_id"
				   ,$"prcp_ctgry_cd"
				   ,$"prcp_type_cd"
				   ,$"hc_id"
				   ,$"sbscrbr_id"
				   ,$"scrty_lvl_cd"
				   ,$"prod_ofrg_key"
				   ,$"mdcl_expsr_nbr"
				   ,$"phrmcy_mbr_expsr_nbr"
				   ,$"dntl_expsr_nbr"
				   ,$"vsn_expsr_nbr"
				   ,$"PHRMCY_MBR_CVRG_CNT"
				   ,$"MBU_CF_CD"
           ,$"CDHP_CTGRY_CD"
           ,$"cii_rmm_flg")


          .where(trim($"RCRD_STTS_CD")!=="DEL")


          .select(trim($"PRCHSR_ORG_CTGRY_TYPE_CD").as("PRCHSR_ORG_CTGRY_TYPE_CD"),
                  trim($"MBRSHP_SOR_CD").as("MBRSHP_SOR_CD"),
                  trim($"PRCHSR_ORG_NBR").as("PRCHSR_ORG_NBR"),
                   trim($"PRCHSR_ORG_TYPE_CD").as("PRCHSR_ORG_TYPE_CD"),
                  $"RCRD_STTS_CD" )


val FRAME1= FLTR.as("FLTR").join(DMGRPHC.as("DMG"),trim($"FLTR.MBRSHP_SOR_CD")===$"DMG.MBRSHP_SOR_CD" &&
                 $"FLTR.RLTD_PRCHSR_ORG_NBR"===$"DMG.PRCHSR_ORG_NBR" &&
                 $"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"===$"DMG.PRCHSR_ORG_TYPE_CD","left_outer")


                 .select(
                          $"FLTR.PRCHSR_ORG_NBR"
                         ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
                         ,$"FLTR.prod_id"
                         ,$"FLTR.BNFT_PKG_ID"
                         ,$"FLTR.PKG_NBR"
                         ,$"FLTR.MBRSHP_SOR_CD"
                         ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
                         ,$"FLTR.PROD_SOR_CD"
                         ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
                         ,$"FLTR.MBR_KEY"
                         ,$"FLTR.BNFT_PKG_KEY"
                         ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
                         ,$"DMG.PRCHSR_ORG_CTGRY_TYPE_CD"
                         ,$"DMG.PRCHSR_ORG_TYPE_CD"
                         ,$"DMG.RCRD_STTS_CD"
                         ,$"SGMNTN_SUBGRP_ID"
                         ,$"prod_id1"
                         ,$"SGMNTN_SOR_CD"
                         ,$"GRP_STTS_CD",$"MAX_FUNDG_CF_CD"
                         ,$"GRP_PLAN_CD"
						 ,$"FLTR.FUNDG_CF_CD"
						 ,$"FLTR.src_grp_nbr"
						 ,$"FLTR.mdcl_mbr_cvrg_cnt"
						 ,$"FLTR.vsn_mbr_cnt"
						 ,$"FLTR.dntl_mbr_cnt"
						 ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
						 ,$"FLTR.cntrct_type_cd"
						 ,$"FLTR.othr_insrnc_type_cd"
						 ,$"FLTR.gndr_cd"
						 ,$"FLTR.mbr_mnth_cob_cd"
						 ,$"FLTR.prcp_id"
						 ,$"FLTR.prcp_ctgry_cd"
						 ,$"FLTR.prcp_type_cd"
						 ,$"FLTR.hc_id"
						 ,$"FLTR.sbscrbr_id"
						 ,$"FLTR.scrty_lvl_cd"
						 ,$"FLTR.prod_ofrg_key"
						 ,$"FLTR.mdcl_expsr_nbr"
						 ,$"FLTR.phrmcy_mbr_expsr_nbr"
						 ,$"FLTR.dntl_expsr_nbr"
						 ,$"FLTR.vsn_expsr_nbr"
						 ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
						 ,$"FLTR.MBU_CF_CD"
             ,$"CDHP_CTGRY_CD"
             ,$"FLTR.CII_RMM_FLG")


      //.select($"PROD_OFRG_KEY",$"PROD_OFRG_NTWK_EFCTV_DT",$"PROD_OFRG_NTWK_TRMNTN_DT",$"MAX_NTWK_ID".as("NTWK_ID"))


  val FRAME1_A  = FRAME1.as("FLTR").join(WRK_PROD.as("INTRM_PRODCT"),trim($"FLTR.GRP_PLAN_CD")===$"INTRM_PRODCT.GRP_PLAN_CD" &&
                           $"FLTR.MBRSHP_SOR_CD"===$"INTRM_PRODCT.MBRSHP_SOR_CD" , "left_outer")


                      .join(WRK_ECR_SCD_PROD.as("WRK_ECR_SCD_PROD"),trim($"FLTR.PROD_ID")===$"WRK_ECR_SCD_PROD.PROD_ID" &&
                           $"FLTR.PROD_SOR_CD"===$"WRK_ECR_SCD_PROD.PROD_SOR_CD" , "left_outer")


                      .join(PROD_OFRG_NTWK.as("PROD_OFRG_NTWK"),trim($"FLTR.PROD_OFRG_KEY")===$"PROD_OFRG_NTWK.PROD_OFRG_KEY" &&
                           $"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"===$"PROD_OFRG_NTWK.NTWK_DT" , "left_outer")


         .select(
           $"FLTR.PRCHSR_ORG_NBR"
           ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
           ,$"FLTR.prod_id"
           ,$"FLTR.BNFT_PKG_ID"
           ,$"FLTR.PKG_NBR"
           ,$"FLTR.MBRSHP_SOR_CD"
           ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
           ,$"FLTR.PROD_SOR_CD"
           ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
           ,$"FLTR.MBR_KEY"
           ,$"FLTR.BNFT_PKG_KEY"
           ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
           ,$"PRCHSR_ORG_CTGRY_TYPE_CD"
           ,$"PRCHSR_ORG_TYPE_CD"
           ,$"RCRD_STTS_CD"
           ,$"SGMNTN_SUBGRP_ID"
           ,$"prod_id1"
           ,$"SGMNTN_SOR_CD"
           ,$"GRP_STTS_CD"
           ,$"MAX_FUNDG_CF_CD"
           ,$"FLTR.GRP_PLAN_CD"
           ,$"FLTR.FUNDG_CF_CD"
           ,$"FLTR.src_grp_nbr"
           ,$"FLTR.mdcl_mbr_cvrg_cnt"
           ,$"FLTR.vsn_mbr_cnt"
           ,$"FLTR.dntl_mbr_cnt"
           ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
           ,$"FLTR.cntrct_type_cd"
           ,$"FLTR.othr_insrnc_type_cd"
           ,$"FLTR.gndr_cd"
           ,$"FLTR.mbr_mnth_cob_cd"
           ,$"FLTR.prcp_id"
           ,$"FLTR.prcp_ctgry_cd"
           ,$"FLTR.prcp_type_cd"
           ,$"FLTR.hc_id"
           ,$"FLTR.sbscrbr_id"
           ,$"FLTR.scrty_lvl_cd"
           ,$"FLTR.prod_ofrg_key"
           ,$"FLTR.mdcl_expsr_nbr"
           ,$"FLTR.phrmcy_mbr_expsr_nbr"
           ,$"FLTR.dntl_expsr_nbr"
           ,$"FLTR.vsn_expsr_nbr"
           ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
           ,$"FLTR.MBU_CF_CD"
         ,$"PLAN_GRP_ID"
         ,$"CDHP_CTGRY_CD1".as("CDHP_CTGRY_CD")


val FRAME2= FRAME1_A.as("F1").join(broadcast(PLAN_REF).as("PLRF"),$"F1.MBRSHP_SOR_CD"=== $"PLRF.SOR_CD","left_outer")


           .join(WRK_PROD.as("WPRD"),$"F1.GRP_PLAN_CD"=== $"WPRD.GRP_PLAN_CD" && $"F1.MBRSHP_SOR_CD"=== $"WPRD.MBRSHP_SOR_CD","left_outer")


		   .join(df_prd.as("prd"),$"F1.PROD_SOR_CD"=== $"prd.PROD_SOR_CD" && $"F1.PROD_ID" === $"prd.PROD_ID" ,"left_outer")


                      .select(
                       $"F1.PRCHSR_ORG_NBR"
                      ,$"F1.RLTD_PRCHSR_ORG_NBR"
                      ,$"F1.prod_id"
                      ,$"F1.BNFT_PKG_ID"
                      ,$"F1.PKG_NBR"
                      ,$"F1.MBRSHP_SOR_CD"
                      ,$"F1.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"F1.PROD_SOR_CD"
                      ,$"F1.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"F1.MBR_KEY"
                      ,$"F1.BNFT_PKG_KEY"
                      ,$"F1.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"F1.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"F1.PRCHSR_ORG_TYPE_CD"
                      ,$"F1.RCRD_STTS_CD"
                      ,$"F1.SGMNTN_SUBGRP_ID"
                      ,$"F1.prod_id1"
                      ,$"F1.SGMNTN_SOR_CD"
                      ,$"F1.GRP_STTS_CD"
                      ,when($"HLTH_PROD_SRVC_TYPE_CD1".isin("","~01","~02","~03","~NA","~DV","UNK"),"NA").otherwise($"HLTH_PROD_SRVC_TYPE_CD1").as("HLTH_PROD_SRVC_TYPE_CD")


//segmentation adding the source columns in 0th sql from emplymnt. Just need to select here  in df_EMPLR_GRP_DEPT

val df_emp=getSparkSession.sql(getFileContent(aDataSrcCfg.environment.concat("/").concat(sqls(1))).replace("${env}", aDataSrcCfg.env).replace("${release}", aDataSrcCfg.release).replace("${layer}", aDataSrcCfg.layer)) //cii_dept.sql

val df_EMPLR_GRP_DEPT= FRAME2.as("FLTR").join(df_emp.as("DPT"),
$"FLTR.MBR_KEY" === $"DPT.MBR_KEY" && $"FLTR.MBRSHP_SOR_CD" === $"DPT.MBRSHP_SOR_CD"
&& $"FLTR.RLTD_PRCHSR_ORG_NBR" === $"DPT.PRCHSR_ORG_NBR"

 && $"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"=== $"DPT.ELGBLTY_CLNDR_MNTH_END_DT","left_outer")


.select(
    $"FLTR.PRCHSR_ORG_NBR"
                      ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
                      ,$"FLTR.prod_id"
                      ,$"FLTR.BNFT_PKG_ID"
                      ,$"FLTR.PKG_NBR"
                      ,$"FLTR.MBRSHP_SOR_CD"
                      ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.PROD_SOR_CD"
                      ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"FLTR.MBR_KEY"
                      ,$"FLTR.BNFT_PKG_KEY"
                      ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"FLTR.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"FLTR.PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.RCRD_STTS_CD"
                      ,$"FLTR.SGMNTN_SUBGRP_ID"
                      ,$"FLTR.prod_id1"
                      ,$"FLTR.SGMNTN_SOR_CD"
                      ,$"FLTR.GRP_STTS_CD"
                      ,$"FLTR.HLTH_PROD_SRVC_TYPE_CD"
                      ,$"FLTR.PLAN_ID1"
                      ,trim($"EMPLR_DEPT_NBR").as("EMPLR_DEPT_NBR")


val df_rlshp12= df_EMPLR_GRP_DEPT.as("FLTR").join(df_rlshp.as("RLSHP"),
      $"FLTR.MBRSHP_SOR_CD" === $"RLSHP.MBRSHP_SOR_CD" && $"FLTR.PRCHSR_ORG_NBR" === $"RLSHP.RLTD_PRCHSR_ORG_NBR"
      && $"FLTR.ELGBLTY_CLNDR_MNTH_END_DT" === $"RLSHP.ELGBLTY_CLNDR_MNTH_END_DT"
  ,"left_outer")


  .select(
    $"FLTR.PRCHSR_ORG_NBR"
                      ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
                      ,$"FLTR.prod_id"
                      ,$"FLTR.BNFT_PKG_ID"
                      ,$"FLTR.PKG_NBR"
                      ,$"FLTR.MBRSHP_SOR_CD"
                      ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.PROD_SOR_CD"
                      ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"FLTR.MBR_KEY"
                      ,$"FLTR.BNFT_PKG_KEY"
                      ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"FLTR.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"FLTR.PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.RCRD_STTS_CD"
                      ,$"FLTR.SGMNTN_SUBGRP_ID"
                      ,$"FLTR.prod_id1"
                      ,$"FLTR.SGMNTN_SOR_CD"
                      ,$"FLTR.GRP_STTS_CD"
                      ,$"FLTR.HLTH_PROD_SRVC_TYPE_CD"
                      ,$"FLTR.PLAN_ID1"
                      ,$"FLTR.EMPLR_DEPT_NBR"
                      ,$"FLTR.MAX_FUNDG_CF_CD"
                      ,$"SGMNTN_ASSN_ID"
					  ,$"FLTR.FUNDG_CF_CD"
					 ,$"FLTR.src_grp_nbr"
				     ,$"FLTR.mdcl_mbr_cvrg_cnt"
				     ,$"FLTR.vsn_mbr_cnt"
				     ,$"FLTR.dntl_mbr_cnt"
				     ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
				     ,$"FLTR.cntrct_type_cd"
				     ,$"FLTR.othr_insrnc_type_cd"
				     ,$"FLTR.gndr_cd"
				     ,$"FLTR.mbr_mnth_cob_cd"
				     ,$"FLTR.prcp_id"
				     ,$"FLTR.prcp_ctgry_cd"
				     ,$"FLTR.prcp_type_cd"
				     ,$"FLTR.hc_id"
				     ,$"FLTR.sbscrbr_id"
				     ,$"FLTR.scrty_lvl_cd"
				     ,$"FLTR.prod_ofrg_key"
				     ,$"FLTR.mdcl_expsr_nbr"
				     ,$"FLTR.phrmcy_mbr_expsr_nbr"
				     ,$"FLTR.dntl_expsr_nbr"
				     ,$"FLTR.vsn_expsr_nbr"
				     ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
				     ,$"FLTR.RX_OVERRIDE_FLAG"
					 ,$"FLTR.ORIG_HLTH_PROD_SRVC_TYPE_CD"
					 ,$"FLTR.MBU_CF_CD"
    ,$"FLTR.CLM_CD"
    ,$"FLTR.CLM_RPTG_1_CD"
    ,$"FLTR.CLM_RPTG_2_CD"
    ,$"FLTR.CLM_RPTG_3_CD"
    ,$"FLTR.EMP_NBR"
    ,$"FLTR.PLAN_GRP_ID"
    ,$"FLTR.CDHP_CTGRY_CD"
    ,$"FLTR.MBR_NTWK_ID"
    ,$"FLTR.CII_RMM_FLG"
    )


  .join(df_rlshp2.as("RLSHP2"),
      $"FLTR.MBRSHP_SOR_CD" === $"RLSHP2.MBRSHP_SOR_CD" && $"FLTR.PRCHSR_ORG_NBR" === $"RLSHP2.RLTD_PRCHSR_ORG_NBR"
      && $"FLTR.ELGBLTY_CLNDR_MNTH_END_DT" === $"RLSHP2.ELGBLTY_CLNDR_MNTH_END_DT"
  ,"left_outer")


  .select(
    $"FLTR.PRCHSR_ORG_NBR"
                      ,$"FLTR.RLTD_PRCHSR_ORG_NBR"
                      ,$"FLTR.prod_id"
                      ,$"FLTR.BNFT_PKG_ID"
                      ,$"FLTR.PKG_NBR"
                      ,$"FLTR.MBRSHP_SOR_CD"
                      ,$"FLTR.RLTD_PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.PROD_SOR_CD"
                      ,$"FLTR.ELGBLTY_CLNDR_MNTH_END_DT"
                      ,$"FLTR.MBR_KEY"
                      ,$"FLTR.BNFT_PKG_KEY"
                      ,$"FLTR.MBR_PROD_ENRLMNT_EFCTV_DT"
                      ,$"FLTR.PRCHSR_ORG_CTGRY_TYPE_CD"
                      ,$"FLTR.PRCHSR_ORG_TYPE_CD"
                      ,$"FLTR.RCRD_STTS_CD"
                      ,$"FLTR.SGMNTN_SUBGRP_ID"
                      ,$"FLTR.prod_id1"
                      ,$"FLTR.SGMNTN_SOR_CD"
                      ,$"FLTR.GRP_STTS_CD"
                      ,$"FLTR.HLTH_PROD_SRVC_TYPE_CD"
                      ,$"FLTR.PLAN_ID1"
                      ,$"FLTR.EMPLR_DEPT_NBR"
                      ,$"FLTR.MAX_FUNDG_CF_CD"
                      ,$"SGMNTN_ASSN_ID"
                      ,$"SGMNTN_CLNT_ID"
,$"FLTR.FUNDG_CF_CD"
					 ,$"FLTR.src_grp_nbr"
				     ,$"FLTR.mdcl_mbr_cvrg_cnt"
				     ,$"FLTR.vsn_mbr_cnt"
				     ,$"FLTR.dntl_mbr_cnt"
				     ,$"FLTR.mbr_prod_enrlmnt_trmntn_dt"
				     ,$"FLTR.cntrct_type_cd"
				     ,$"FLTR.othr_insrnc_type_cd"
				     ,$"FLTR.gndr_cd"
				     ,$"FLTR.mbr_mnth_cob_cd"
				     ,$"FLTR.prcp_id"
				     ,$"FLTR.prcp_ctgry_cd"
				     ,$"FLTR.prcp_type_cd"
				     ,$"FLTR.hc_id"
				     ,$"FLTR.sbscrbr_id"
				     ,$"FLTR.scrty_lvl_cd"
				     ,$"FLTR.prod_ofrg_key"
				     ,$"FLTR.mdcl_expsr_nbr"
				     ,$"FLTR.phrmcy_mbr_expsr_nbr"
				     ,$"FLTR.dntl_expsr_nbr"
				     ,$"FLTR.vsn_expsr_nbr"
				     ,$"FLTR.PHRMCY_MBR_CVRG_CNT"
				     ,$"FLTR.RX_OVERRIDE_FLAG"
					 ,$"FLTR.ORIG_HLTH_PROD_SRVC_TYPE_CD"
					 ,$"FLTR.MBU_CF_CD"
    ,$"FLTR.CLM_CD"
    ,$"FLTR.CLM_RPTG_1_CD"
    ,$"FLTR.CLM_RPTG_2_CD"
    ,$"FLTR.CLM_RPTG_3_CD"
    ,$"FLTR.EMP_NBR"
    ,$"FLTR.PLAN_GRP_ID"
    ,$"FLTR.CDHP_CTGRY_CD"
    ,$"FLTR.MBR_NTWK_ID"
    ,$"FLTR.CII_RMM_FLG"
    )


val df_wrk_wth_insrd= df_rlshp13.as("FLT1").join(broadcast(df_bot_FULLY_INSRD).as("bot_FULLY_INSRD"),$"bot_FULLY_INSRD.FUNDG_CF_CD" === $"FLT1.MAX_FUNDG_CF_CD"
        ,"left_outer")


       .select(
    upper($"FLT1.PRCHSR_ORG_NBR").as("SGMNTN_GRP_ID")


  val tbl_mbr_cob =CIIUtilities.readDFfromS3(aDataSrcCfg,"df_mbr_cob")


  val df_wrk= df_wrk_wth_insrd.as("FLT1").join(tbl_mbr_cob.as("cob_data"),$"FLT1.MBR_KEY" === $"cob_data.MBR_KEY"
    && $"FLT1.ELGBLTY_CLNDR_MNTH_END_DT" === $"cob_data.LAST_DT_OF_THE_MNTH_DT" , "left_outer")


    .select(
       $"FLT1.SGMNTN_GRP_ID"
      ,$"FLT1.RLTD_PRCHSR_ORG_NBR"
      ,$"FLT1.prod_id"
      ,$"FLT1.BNFT_PKG_ID"
      ,$"FLT1.PLAN_ID"
      ,$"FLT1.PKG_NBR"
      ,$"FLT1.MBRSHP_SOR_CD"
      ,$"FLT1.RLTD_PRCHSR_ORG_TYPE_CD"
      ,$"FLT1.PROD_SOR_CD"
      ,$"FLT1.ELGBLTY_CLNDR_MNTH_END_DT"
      ,$"FLT1.MBR_KEY"
      ,$"FLT1.BNFT_PKG_KEY"
      ,$"FLT1.MBR_PROD_ENRLMNT_EFCTV_DT"
      ,$"FLT1.SGMNTN_SUBGRP_ID"
      ,$"FLT1.prod_id1"
      ,$"FLT1.SGMNTN_SOR_CD"
      ,$"FLT1.GRP_STTS_CD"
      ,$"FLT1.HLTH_PROD_SRVC_TYPE_CD"
      ,$"FLT1.EMPLR_DEPT_NBR"
      ,$"FLT1.MAX_FUNDG_CF_CD"
      ,$"FLT1.SGMNTN_ASSN_ID"
      ,$"FLT1.SGMNTN_CLNT_ID"
      ,$"FULLY_INSRD_CD"
      ,$"FLT1.FUNDG_CF_CD"
      ,$"FLT1.src_grp_nbr"
      ,$"FLT1.mdcl_mbr_cvrg_cnt"
      ,$"FLT1.vsn_mbr_cnt"
      ,$"FLT1.dntl_mbr_cnt"
      ,$"FLT1.mbr_prod_enrlmnt_trmntn_dt"
      ,$"FLT1.cntrct_type_cd"
      ,$"FLT1.othr_insrnc_type_cd"
      ,$"FLT1.gndr_cd"
      ,$"FLT1.mbr_mnth_cob_cd"
      ,$"FLT1.prcp_id"
      ,$"FLT1.prcp_ctgry_cd"
      ,$"FLT1.prcp_type_cd"
      ,$"FLT1.hc_id"
      ,$"FLT1.sbscrbr_id"
      ,$"FLT1.scrty_lvl_cd"
      ,$"FLT1.prod_ofrg_key"
      ,$"FLT1.mdcl_expsr_nbr"
      ,$"FLT1.phrmcy_mbr_expsr_nbr"
      ,$"FLT1.dntl_expsr_nbr"
      ,$"FLT1.vsn_expsr_nbr"
      ,$"FLT1.PHRMCY_MBR_CVRG_CNT"
      ,$"FLT1.RX_OVERRIDE_FLAG"
      ,$"FLT1.ORIG_HLTH_PROD_SRVC_TYPE_CD"
      ,$"FLT1.MBU_CF_CD"
      ,$"FLT1.CLM_CD"
      ,$"FLT1.CLM_RPTG_1_CD"
      ,$"FLT1.CLM_RPTG_2_CD"
      ,$"FLT1.CLM_RPTG_3_CD"
      ,$"FLT1.EMP_NBR"
      ,$"FLT1.PLAN_GRP_ID"
      ,$"FLT1.CDHP_CTGRY_CD"
      ,$"FLT1.MBR_NTWK_ID"
      ,$"PRMRY_CVRG_CD"
      ,$"MEDCR_CD"
      ,$"FLT1.CII_RMM_FLG"
    )


          .join(df_RLTNSHP.as("RLTNSHP"), $"RLTNSHP.ACCT_ID" === $"SCD.SCD_ORG_ID")


          .where(trim($"SCD.SCD_ORG_TYPE_CD")==="WAC")


          .where($"SCD.RLTD_SCD_ORG_TYPE_CD".isin("GRP","CLNT","ASSN"))


          .select(trim($"SCD.RLTD_SCD_ORG_SOR_CD").as("RLTD_SCD_ORG_SOR_CD"),trim($"SCD.RLTD_SCD_ORG_ID").as("RLTD_SCD_ORG_ID"),  trim($"SCD.SCD_ORG_ID").as("SCD_ORG_ID"),trim($"SCD.RLTD_SCD_ORG_TYPE_CD").as("RLTD_SCD_ORG_TYPE_CD"))


val FRAME4_1= df_wrk.as("F3").join(SCD_RLTNSHP_GRP.as("SCD1"),$"F3.SGMNTN_SOR_CD" === $"SCD1.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_GRP_ID" === $"SCD1.RLTD_SCD_ORG_ID").filter($"cii_rmm_flg"=== "Y")


                                        .select(
                                        $"F3.SGMNTN_ASSN_ID"
                                       ,$"F3.SGMNTN_CLNT_ID"
                                       ,$"F3.SGMNTN_GRP_ID"
                                       ,$"F3.SGMNTN_SUBGRP_ID"
                                       ,$"F3.EMPLR_DEPT_NBR"
                                       ,$"F3.FULLY_INSRD_CD"
                                       ,$"F3.GRP_STTS_CD"
                                       ,$"F3.HLTH_PROD_SRVC_TYPE_CD"
                                       ,$"F3.prod_id1".as("prod_id")


val FRAME4_2= df_wrk.as("F3") .join(SCD_RLTNSHP_CLNT.as("SCD2"),$"F3.SGMNTN_SOR_CD" === $"SCD2.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_CLNT_ID" === $"SCD2.RLTD_SCD_ORG_ID").filter($"cii_rmm_flg"=== "Y").select(
                                        $"F3.SGMNTN_ASSN_ID"
                                       ,$"F3.SGMNTN_CLNT_ID"
                                       ,$"F3.SGMNTN_GRP_ID"
                                       ,$"F3.SGMNTN_SUBGRP_ID"
                                       ,$"F3.EMPLR_DEPT_NBR"
                                       ,$"F3.FULLY_INSRD_CD"
                                       ,$"F3.GRP_STTS_CD"
                                       ,$"F3.HLTH_PROD_SRVC_TYPE_CD"
                                       ,$"F3.prod_id1".as("prod_id")


val FRAME4_3= df_wrk.as("F3").join(SCD_RLTNSHP_ASSN.as("SCD3"),$"F3.SGMNTN_SOR_CD" === $"SCD3.RLTD_SCD_ORG_SOR_CD"
            && $"F3.SGMNTN_ASSN_ID" === $"SCD3.RLTD_SCD_ORG_ID").filter($"cii_rmm_flg"=== "Y").select(
                                        $"F3.SGMNTN_ASSN_ID"
                                       ,$"F3.SGMNTN_CLNT_ID"
                                       ,$"F3.SGMNTN_GRP_ID"
                                       ,$"F3.SGMNTN_SUBGRP_ID"
                                       ,$"F3.EMPLR_DEPT_NBR"
                                       ,$"F3.FULLY_INSRD_CD"
                                       ,$"F3.GRP_STTS_CD"
                                       ,$"F3.HLTH_PROD_SRVC_TYPE_CD"
                                       ,$"F3.prod_id1".as("prod_id")


val FRAME5= FRAME4.as("F4").join(broadcast(scd_org).as("srg"),$"F4.ACCT_ID"=== $"srg.ACCT_ID","left_outer")


					.select($"F4.ACCT_ID"
	                          ,$"F4.SGMNTN_ASSN_ID"
	                          ,$"F4.SGMNTN_CLNT_ID"
	                          ,$"F4.SGMNTN_GRP_ID"
	                          ,$"F4.SGMNTN_SUBGRP_ID"
	                          ,$"F4.EMPLR_DEPT_NBR"
	                          ,$"F4.FULLY_INSRD_CD"
	                          ,$"F4.GRP_STTS_CD"
	                          ,$"F4.HLTH_PROD_SRVC_TYPE_CD"
                            ,$"ORIG_HLTH_PROD_SRVC_TYPE_CD"
	                          ,when($"F4.prod_id".isin("","~01","~02","~03","~NA","~DV","UNK") || $"F4.prod_id".isNull,"NA").otherwise(upper(trim(regexp_replace($"F4.prod_id","\\^", "\\s")))).as("prod_id")


              //val FRAME5=CIIUtilities.readDFfromS3(aDataSrcCfg, "FRAME5_combined_new")


					.select($"F4.ACCT_ID"
	                          ,$"F4.SGMNTN_ASSN_ID"
	                          ,$"F4.SGMNTN_CLNT_ID"
	                          ,$"F4.SGMNTN_GRP_ID"
	                          ,$"F4.SGMNTN_SUBGRP_ID"
	                          ,$"F4.EMPLR_DEPT_NBR"
	                          ,$"F4.FULLY_INSRD_CD"
	                          ,$"F4.GRP_STTS_CD"
	                          ,$"F4.HLTH_PROD_SRVC_TYPE_CD"
                            ,$"ORIG_HLTH_PROD_SRVC_TYPE_CD"
	                          ,$"F4.prod_id"
	                          ,$"F4.PLAN_ID"
	                          ,$"F4.SGMNTN_SOR_CD"
	                         ,$"RPTG_OFSHR_ACSBL_IND"
	                          ,$"SCRTY_LVL_CD"
	                        ,$"mbr_key"
							,$"elgblty_clndr_mnth_end_dt"
							,$"mbrshp_sor_cd"
							,$"src_grp_nbr"
							,$"rltd_prchsr_org_nbr"
							,$"prchsr_org_nbr"
							,$"fundg_cf_cd"
							,$"prod_sor_cd"
							,$"prod_id_fltr"
							,$"bnft_pkg_id"
							,$"pkg_nbr"
							,$"mdcl_mbr_cvrg_cnt"
							,$"vsn_mbr_cnt"
							,$"dntl_mbr_cnt"
							,$"mbr_prod_enrlmnt_efctv_dt"
							,$"mbr_prod_enrlmnt_trmntn_dt"
							,$"cntrct_type_cd"
							,$"othr_insrnc_type_cd"
							,$"gndr_cd"
							,$"mbr_mnth_cob_cd"
							,$"prcp_id"
							,$"prcp_ctgry_cd"
							,$"bnft_pkg_key"
							,$"rltd_prchsr_org_type_cd"
							,$"prcp_type_cd"
							,$"hc_id"
							,$"sbscrbr_id"
							,$"scrty_lvl_cd_fltr"
							,$"prod_ofrg_key"
							,$"mdcl_expsr_nbr"
							,$"phrmcy_mbr_expsr_nbr"
							,$"dntl_expsr_nbr"
							,$"vsn_expsr_nbr"
							,$"MAX_FUNDG_CF_CD"
							,$"PHRMCY_MBR_CVRG_CNT"
							,$"hc_id".as("hcid_key")


					.select(lit("NA").as("ACCT_ID")


  val frame5_new =CIIUtilities.readDFfromS3(aDataSrcCfg,"FRAME6")
