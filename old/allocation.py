
import duckdb as db
import pandas as pd
import streamlit as st


class Allocation(object) :
    def __init__(self, *args ) -> None:
        
        __con = db.connect()
        self.con = __con.cursor()
        self.Tranche = args[0]
        self.PL = args[1]
        self.Tranpath = args[2]
        self.TrancheFile = args[2] + "TrancheData.parquet"
        self.TrancheFile = f"'{self.TrancheFile}'"
        self.PLFile = args[2] + "PL.parquet"
        self.PLFile = f"'{self.PLFile}'"
        
    
    def __PerAllocGAV(self) -> pd.DataFrame:
        df_tranche = self.Tranche

        df = self.con.execute(f"""

                select AllocationClass , sum(cast(AdjustedBeginningGross as numeric(15,2))) AS AdjustedBeginningGross  
                from df_tranche
                group by  AllocationClass        

                """
        ).df()
        return  df

    def __GetDatawithEP(self):
        df_Sum_Gav = self.__PerAllocGAV()
        df_tranche = self.Tranche

        df = self.con.execute(f"""
             select t.AllocationClass , t.AccountSegmentID , cast(t.AdjustedBeginningGross as numeric(15,2)) AS AdjustedBeginningGross,
             ts.AdjustedBeginningGross AS SUMOFGAVPerAllocClass, 
             round(cast(cast(t.AdjustedBeginningGross as numeric(15,2)) / cast(ts.AdjustedBeginningGross as numeric(15,2)) as numeric(30,28)),18)   AS EP                 
             from df_tranche t  inner join df_Sum_Gav ts
             on t.AllocationClass = ts.AllocationClass
                                                              
        """).df()

        return df
    
    def __RearrangeEP(self):
        df_tranche_ep = self.__GetDatawithEP()

        df = self.con.execute(f"""
                  select AllocationClass , sum(round(cast(EP as numeric(30,28)),18)) AS EPSUM,
                  1 - sum(round(cast(EP as numeric(30,28)),18)) AS Diff            
                  from   df_tranche_ep          
                  group by AllocationClass
                              
                                        
        """).df()

        df_maxep = self.con.execute(f"""
               select AllocationClass , MAX(round(cast(EP as numeric(30,28)),18)) AS MAXEP 
               from  df_tranche_ep
               group by   AllocationClass                   

        """).df()

        
        df_max_Tranche = self.con.execute(f"""
                
                         select tr.AllocationClass , tr.AccountSegmentID , tr.EP , mep.MAXEP,
                         row_number() OVER (PARTITION BY tr.AllocationClass   order by tr.AllocationClass ) AS cnt                                             
                         from df_tranche_ep tr 
                         inner join df_maxep mep on
                         tr.AllocationClass = mep.AllocationClass and tr.EP = mep.MAXEP
                         
                                       
        """).df()



        df_max_SingleTranche = self.con.execute(f"""
                select * from df_max_Tranche where cnt = 1
        """).df()

        
        df_makeSumepone = self.con.execute(f"""
                select mep.AllocationClass , 
                mep.AccountSegmentID, 
                mep.MAXEP , d.Diff,  round(cast(mep.MAXEP + d.Diff as  numeric(30,28)),18) AS FinalMaxEp
                from df_max_SingleTranche mep inner join  df d
                on  mep.AllocationClass = d.AllocationClass                                                                       
        """).df()
        #, dem.AccountSegmentID as MaxAccountSegmentID

        df_data_ep = self.con.execute(f"""
                select t.AllocationClass , t.AccountSegmentID , t.AdjustedBeginningGross,
                coalesce(dem.FinalMaxEp, t.EP) AS EP , dem.AccountSegmentID as MaxAccountSegmentID                      
                from df_tranche_ep t
                left join df_makeSumepone dem
                on lower(t.AllocationClass) = lower(dem.AllocationClass)
                and t.AccountSegmentID = dem.AccountSegmentID                                                                  
        """).df()

        return df_data_ep
        

    def PLAllocation(self) -> pd.DataFrame :
        EP_data = self.__RearrangeEP()
        
        PL_Data_Raw = self.PL
        PL_Data = self.con.execute(f"""

                select lower(AllocationClass) AS AllocationClass , AllocationClassCode , LedgerAccount,
                sum(round(coalesce(cast(Amount as numeric(20,8)) ,0),6)) AS Amount,
                from  PL_Data_Raw
                group by  lower(AllocationClass) , AllocationClassCode , LedgerAccount                                                                                             
        
        """).df()

        Pl_Allocation = self.con.execute(f"""
                select lower(e.AllocationClass) AS AllocationClass , AllocationClassCode,  e.AccountSegmentID , e.AdjustedBeginningGross, round(cast(e.EP as numeric(30,28)),28) AS EP,
                pl.AllocationClassCode, coalesce(pl.LedgerAccount,0) as LedgerAccount , 
                cast(coalesce(( cast(pl.Amount as numeric(12,2)) * cast(e.EP as numeric(30,28)) ),0) as numeric(24,12)) AS AllocatedPL                          
                from EP_data e   left join  PL_Data pl
                on lower(e.AllocationClass) = lower(pl.AllocationClass)                           
         """).df()
        
        
        PL_Allocation_perAccount = self.con.execute(f"""
               select  AllocationClass , LedgerAccount , AllocationClassCode, 
               sum(round(coalesce(cast(AllocatedPL as numeric(24,12)) , 0),12)) AS AllocatedPLSUM
               from  Pl_Allocation
               group by  AllocationClass , LedgerAccount ,AllocationClassCode                                       
                                                                                        
        """).df()

        
        PL_Allocation_diff = self.con.execute(f"""
                select pl_c.AllocationClass , pl_c.LedgerAccount, pl_c.AllocatedPLSUM , 
                round(cast(pl.Amount as numeric(24,12)),12)  AS LedgerAmount,
                round(cast(pl.Amount as numeric(24,12)),12) - round(pl_c.AllocatedPLSUM,12) AS LedgerAmountDiff                               
                from PL_Allocation_perAccount pl_c 
                left join PL_Data  pl on  pl_c.AllocationClass = pl.AllocationClass
                and  pl_c.LedgerAccount = pl.LedgerAccount                                                         
        """).df()
        
        
    
        Pl_Allocation_maxval = self.con.execute(f"""
                
                select AllocationClass , LedgerAccount,   
                coalesce(max(coalesce(cast(AllocatedPL as numeric(24,12)),0)),0) AllocatedPLMAX
                from   Pl_Allocation 
                group by AllocationClass , LedgerAccount
                                               
                                                                                                     
        """).df()


        
        ##tr.AllocatedPL = mep.AllocatedPLMAX
        pl_alloc_max_tranche = self.con.execute(f"""
            select tr.AllocationClass , tr.AccountSegmentID , tr.LedgerAccount,  tr.AllocatedPL , mep.AllocatedPLMAX ,
            row_number() OVER (PARTITION BY tr.AllocationClass,tr.LedgerAccount order by tr.AllocationClass,tr.LedgerAccount ) AS cnt                                             
            from Pl_Allocation tr 
            inner join Pl_Allocation_maxval mep on
            lower(tr.AllocationClass) = lower(mep.AllocationClass) 
            and tr.LedgerAccount = mep.LedgerAccount
            and tr.AllocatedPL = mep.AllocatedPLMAX                                    
            
            
        """).df()

        

        pl_alloc_max_tranche_single = self.con.execute(f"""
             select * from  pl_alloc_max_tranche where cnt = 1  
        """).df()

        
    
        allocated_pl_adjusted = self.con.execute(f"""
                select ap.AllocationClass , ap.AccountSegmentID , ap.LedgerAccount , ap.AllocatedPLMAX, cast(pldiff.LedgerAmountDiff as numeric(24,12)) as diff,
                round(ap.AllocatedPLMAX,12) + round(pldiff.LedgerAmountDiff,12) AS AdjustedPLAllocated                                 
                from  pl_alloc_max_tranche_single ap  
                inner join  PL_Allocation_diff pldiff  
                on lower(ap.AllocationClass) = lower(pldiff.AllocationClass)
                and ap.LedgerAccount = pldiff.LedgerAccount 
                                                                                                                                               
        """ ).df()

        

        df_plalloc_final = self.con.execute(f"""
                select t.AllocationClass , t.AllocationClassCode, t.AccountSegmentID , t.AdjustedBeginningGross,
                round(cast(t.EP as numeric(30,28)),18) AS EP, t.LedgerAccount , 
                round(cast(coalesce(dem.AdjustedPLAllocated,t.AllocatedPL) as numeric(24,12)),12) AS AllocatedPL , 
                t.AllocatedPL as actual                  
                from Pl_Allocation t
                left join allocated_pl_adjusted dem
                on lower(t.AllocationClass) = lower(dem.AllocationClass)
                and t.AccountSegmentID = dem.AccountSegmentID
                and t.LedgerAccount = dem.LedgerAccount
                order by t.AllocationClass , t.LedgerAccount                                                                                                                          
        """).df()

        return df_plalloc_final


    def TrancheData(self) -> pd.DataFrame:
        return self.Tranche
        
        
    
    def TrancheDataMaster(self) -> pd.DataFrame:
        
        df_tranche = self.Tranche

        df = self.con.execute(f"""
            select * from df_tranche where coalesce(TrancheTag,0) = 0
        """).df()

        return df


    def TrancheDataOnshore(self) -> pd.DataFrame:
        df_tranche = self.Tranche
        df = self.con.execute(f"""
            select * from df_tranche where TrancheTag = 1
        """).df()

        return df

    def TrancheDataOffshore(self) -> pd.DataFrame:
        df_tranche = self.Tranche
        df = self.con.execute(f"""
             select * from df_tranche where TrancheTag = 2
        """).df()

        return df

    def PlData(self) -> pd.DataFrame:
        
        return self.PL

    def PlDataOnshore(self) -> pd.DataFrame:
        df_pl = self.PL
        df = self.con.execute(f"""
            select AllocationClass, AllocationClassCode,LedgerAccount, Amount1 AS Amount
            from df_pl 
            where  coalesce(Amount1,0) <> 0                             

        """).df()
        return df
    
    def PlDataOffshore(self) -> pd.DataFrame:
        df_pl = self.PL
        df = self.con.execute(f"""
            select AllocationClass, AllocationClassCode,LedgerAccount, Amount2 AS Amount
            from df_pl  where  coalesce(Amount2,0) <> 0                             

        """).df()
        return df

    def CombinedDetail(self, df_master : pd.DataFrame, df_onshore : pd.DataFrame , df_offshore : pd.DataFrame ) -> pd.DataFrame :
        return self.__CombinedDetail(df_master, df_onshore, df_offshore )
    
    
    def CombinedPL(self, df_master : pd.DataFrame, df_onshore : pd.DataFrame , df_offshore : pd.DataFrame ) -> pd.DataFrame :    
        df_det =  self.__CombinedDetail(df_master, df_onshore, df_offshore )

        df_finalPL = self.con.execute(f"""
            select AllocationClass , AllocationClassCode, LedgerAccount,
            sum(MasterAllocatedPL)  AS MasterPNL,
            sum(OnShoreAllocatedPL)  AS OnshorePNL,                          
            sum(OffShoreAllocatedPL)  AS offshorePNL
            from df_det                          
            group by AllocationClass , AllocationClassCode, LedgerAccount                                                    
                                                               
                                      
        """).df()

        return df_finalPL  
    
    def CombinedSummary(self, df_master : pd.DataFrame, df_onshore : pd.DataFrame , df_offshore : pd.DataFrame) -> pd.DataFrame :
        df = self.__CombinedDetail(df_master, df_onshore, df_offshore )
        df_sum = self.con.execute(f"""
                select AllocationClass, AllocationClassCode,
                AccountSegmentID ,
                sum(MasterAllocatedPL) as   MasterAllocatedPL ,
                sum(OnShoreAllocatedPL)  as  OnShoreAllocatedPL ,
                sum(OffShoreAllocatedPL)  AS OffShoreAllocatedPL,                                                                                           
                sum(TotalAllocatedPL)  AS TotalAllocatedPL
                from df
                group by  
                AllocationClass, 
                AllocationClassCode,
                AccountSegmentID                                   
        """).df()

        return df_sum


    def __CombinedDetail(self, df_master : pd.DataFrame, df_onshore : pd.DataFrame , df_offshore : pd.DataFrame ) -> pd.DataFrame :
        
        df = self.con.execute(f"""

                select dm.AllocationClass, 
                dm.AllocationClassCode ,
                dm.EP AS MasterEP,
                don.EP AS OnshoreEP,
                dof.EP AS OffshoreEP,                                           
                dm.LedgerAccount , 
                dm.AccountSegmentID,
                coalesce(dm.AllocatedPL,0)  AS MasterAllocatedPL, 
                coalesce(don.AllocatedPL,0)  AS OnShoreAllocatedPL, 
                coalesce(dof.AllocatedPL,0)  AS OffShoreAllocatedPL,
                              
                coalesce(dm.AllocatedPL,0)  + 
                coalesce(don.AllocatedPL,0)  + 
                coalesce(dof.AllocatedPL,0)  AS TotalAllocatedPL              
                              
                              
                from  df_master dm             
                left join df_onshore don on dm.AllocationClassCode = don.AllocationClassCode
                and dm.LedgerAccount = don.LedgerAccount and dm.AccountSegmentID = don.AccountSegmentID

                left join df_offshore dof on dm.AllocationClassCode = dof.AllocationClassCode
                and dm.LedgerAccount = dof.LedgerAccount and dm.AccountSegmentID = dof.AccountSegmentID
                order by  dm.AllocationClass ,  dm.LedgerAccount                                                                   
                              



        """).df()
        return df
    
    def CombinedDetailActual(self, df_master : pd.DataFrame, df_onshore : pd.DataFrame , df_offshore : pd.DataFrame ) -> pd.DataFrame :
        return self.__CombinedDetailActual(df_master, df_onshore, df_offshore )

    def __CombinedDetailActual(self, df_master : pd.DataFrame, df_onshore : pd.DataFrame , df_offshore : pd.DataFrame ) -> pd.DataFrame :
        
        df = self.con.execute(f"""

                select dm.AllocationClass, 
                dm.AllocationClassCode ,
                dm.EP AS MasterEP,
                don.EP AS OnshoreEP,
                dof.EP AS OffshoreEP,                                           
                dm.LedgerAccount , 
                dm.AccountSegmentID,
                coalesce(dm.actual,0)  AS MasterAllocatedPL, 
                coalesce(don.actual,0)  AS OnShoreAllocatedPL, 
                coalesce(dof.actual,0)  AS OffShoreAllocatedPL,
                              
                coalesce(dm.actual,0)  + 
                coalesce(don.actual,0)  + 
                coalesce(dof.actual,0)  AS TotalAllocatedPL              
                              
                              
                from  df_master dm             
                left join df_onshore don on dm.AllocationClassCode = don.AllocationClassCode
                and dm.LedgerAccount = don.LedgerAccount and dm.AccountSegmentID = don.AccountSegmentID

                left join df_offshore dof on dm.AllocationClassCode = dof.AllocationClassCode
                and dm.LedgerAccount = dof.LedgerAccount and dm.AccountSegmentID = dof.AccountSegmentID
                order by  dm.AllocationClass ,  dm.LedgerAccount                                                                   
                              



        """).df()
        return df

        


    
    

    

       
    #return list(zip(self.emp, self.tranche, self.begincap ,self.opcontrib,self.opwithdraw,self.__adjcap(), ep, bookincm,  *bucket_alloc, gav_list))
    
    
    
    


        