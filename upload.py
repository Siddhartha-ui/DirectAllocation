import pandas as pd
import streamlit as st
import os
from dboperation import dboperation

class Upload(object) :
    
    def __init__(self, FileObj = "", user ="", date = "", customfilename = "", calcforder = "") -> None:
         
         __dbop = dboperation(str(st.session_state.user_id_rnd))
         self.dbop = __dbop
         self.FileObj = FileObj
         self.user = user
         self.date = date
         self.customfilename = customfilename
         self.calcforder = calcforder
         self.directory_parquet = os.getcwd() + "\\parquet\\" + str(self.dbop.user) + "\\"
    
    def __GetParquetFolder(self, subfolder : str) -> str:
        if len(self.calcforder) == 0 :
            parquet_dir = self.directory_parquet + "\\" + subfolder ##+ "\\"
        else :
            parquet_dir = self.directory_parquet + "\\" + subfolder + "\\" + str(self.calcforder) ##+ "\\"    
        os.makedirs(parquet_dir,exist_ok=True)
        return parquet_dir

    def getcalcfolder(self) -> str:
        return self.calcforder

    def basefolder(self) -> str:
        return self.directory_parquet

    def GetParquetFolder(self, subfolder : str) -> str :
        return self.__GetParquetFolder(subfolder)
    
    def uploadcalculatedAllocation(self, df : pd.DataFrame,folder_name : str,file_name: str) -> str :
        dir_name = self.__GetParquetFolder(folder_name)
        filename = dir_name + "\\" + file_name + ".parquet"
        df.to_parquet(filename,engine= 'pyarrow')
    
    def ReadParquetFile(self, file_name : str) -> pd.DataFrame :
        
        isExisting = os.path.exists(file_name)
        if isExisting :
            
            df = pd.read_parquet(file_name)
            
        else :
            df = pd.DataFrame()
        return df
    
    def xlsx_to_parquet(self) :

        if len(self.customfilename.strip()) == 0 :
            fileNM = str(self.FileObj.name)
        else :
           fileNM = self.customfilename.strip()     

        for j in self.FileObj :

            for sheets in pd.ExcelFile(j).sheet_names :
                sheet_f = pd.read_excel(j,sheets)
                sheet_f.columns = sheet_f.columns.str.replace(' ','')
                dataset_name = str(sheets)
                dataset_name = dataset_name.replace(' ','')
                parquet_dir = self.directory_parquet + "\\" + fileNM + "\\"
                os.makedirs(parquet_dir,exist_ok=True)
                partition_col_name = "sheet_name"
                partition_col_value = str(dataset_name) 
                sheet_f.insert(0,partition_col_name, None)
                sheet_f[partition_col_name] = partition_col_value
                parquet_file_name = parquet_dir + dataset_name + ".parquet"
                sheet_f.to_parquet(parquet_file_name,engine= 'pyarrow')
                
                #parquet.write_to_dataset(table=table, root_path=parquet_filename,  partition_cols= [partition_col_name], existing_data_behavior = 'delete_matching')
                

    