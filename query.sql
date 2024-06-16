PRCHSR_ORG_NBR

FLTR.PRCHSR_ORG_NBR

FLT1.SGMNTN_GRP_ID

select($"F4.ACCT_ID"
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
							,$"RX_OVERRIDE_FLAG"
							,$"MBU_CF_CD"
            ,$"F4.CLM_CD"
            ,$"F4.CLM_RPTG_1_CD"
            ,$"F4.CLM_RPTG_2_CD"
            ,$"F4.CLM_RPTG_3_CD"
            ,$"F4.EMP_NBR"
            ,$"F4.PLAN_GRP_ID"
            ,$"F4.CDHP_CTGRY_CD"
            ,$"F4.MBR_NTWK_ID"
            ,$"F4.PRMRY_CVRG_CD"
            ,$"F4.MEDCR_CD"
            ,$"CII_RMM_FLG").distinct()
            
            
            
            
            val FRAME6_2=df_wrk.as("F4").filter($"cii_rmm_flg"=== 'N')
					.select(lit("NA").as("ACCT_ID")
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
	                          ,$"F4.PLAN_ID"
	                          ,$"F4.SGMNTN_SOR_CD"
	                         ,lit("N").as("RPTG_OFSHR_ACSBL_IND")
	                          ,lit("N").as("SCRTY_LVL_CD")
	                        ,$"mbr_key"
							,$"elgblty_clndr_mnth_end_dt"
							,$"mbrshp_sor_cd"
							,$"src_grp_nbr"
							,$"rltd_prchsr_org_nbr"
							,$"SGMNTN_GRP_ID".as("prchsr_org_nbr")
							,$"fundg_cf_cd"
							,$"prod_sor_cd"
							,$"prod_id".as("prod_id_fltr")
							,when($"bnft_pkg_id".isin("","~01","~02","~03","~NA","~DV","UNK") || $"bnft_pkg_id".isNull,"NA").otherwise(upper(trim(regexp_replace($"bnft_pkg_id","\\^", "\\s")))).as("bnft_pkg_id")
							,when($"pkg_nbr".isin("","~01","~02","~03","~NA","~DV","UNK") || $"pkg_nbr".isNull,"NA").otherwise(upper(trim(regexp_replace($"pkg_nbr","\\^", "\\s")))).as("pkg_nbr")
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
							,lit("N").as("scrty_lvl_cd_fltr")
							,$"prod_ofrg_key"
							,$"mdcl_expsr_nbr"
							,$"phrmcy_mbr_expsr_nbr"
							,$"dntl_expsr_nbr"
							,$"vsn_expsr_nbr"
							,$"MAX_FUNDG_CF_CD"
							,$"PHRMCY_MBR_CVRG_CNT"
							,$"hc_id".as("hcid_key")
							,$"RX_OVERRIDE_FLAG"

							,$"MBU_CF_CD"
            ,$"F4.CLM_CD"
            ,$"F4.CLM_RPTG_1_CD"
            ,$"F4.CLM_RPTG_2_CD"
            ,$"F4.CLM_RPTG_3_CD"
            ,$"F4.EMP_NBR"
            ,$"F4.PLAN_GRP_ID"
            ,$"F4.CDHP_CTGRY_CD"
            ,$"F4.MBR_NTWK_ID"
            ,$"F4.PRMRY_CVRG_CD"
            ,$"F4.MEDCR_CD"
            ,$"CII_RMM_FLG").distinct()
            
            
         
              
   
                       
     val FRAME6 =  FRAME6_1.unionAll(FRAME6_2)      

  CIIUtilities.writeDFtoS3(aDataSrcCfg,FRAME6,"FRAME6")

  val frame5_new =CIIUtilities.readDFfromS3(aDataSrcCfg,"FRAME6")