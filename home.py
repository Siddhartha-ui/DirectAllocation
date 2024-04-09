import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from login import Login
from datetime import datetime
from upload import Upload
from allocation import Allocation
from pathlib import Path
import os
from session import Session
import numpy as np

if __name__ == "__main__":


    st.set_page_config(page_title="Allocation", layout="wide" , page_icon="chart_with_upwards_trend")

    session = Session()    
    

def SignUp():

            login = Login()
            login.create_usertable()

            with st.container() :

                buff, col, col2, buff2 = st.columns([1,2,2,1])
                col.subheader("Create New Account")
                new_user = col.text_input("Username", key= "new_User").strip()
                new_password = col.text_input("Password",type='password', key= "new_password").strip()
                confirm_new_pasword = col.text_input("Confirm New Password",type='password', key= "new_confirm_pass").strip()
                save_btn = col.button("Save")

            if len(new_user) > 0 and len(new_password) > 0 and len(confirm_new_pasword) > 0 and save_btn  :

                    msg = login.PasswordRule(new_password, 8)
                    if len(msg) > 0 :
                       col.error(msg)
                       st.stop()  
                    
                    if login.ValidateConfirmPass(new_password,confirm_new_pasword) :

                        if login.CheckDuplicateuser(new_user) :
                            login.create_usertable()
                            login.add_userdata(new_user,login.make_hashes(new_password))
                            col.success("You have successfully created a valid Account")
                            col.info("Go to Login Menu to login")
                            
                            

                        else :
                            col.error ("User Exists. Create a new user")
                    else :
                        col.error ("New Password and Confirm New Password does not match")
            else :
                   pass                      

def GetCurrentdate() -> str :
    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y")
    return  str(dt_string)




def download_csv(df : pd.DataFrame, rptname = "") :
    csv = convert_df(df)
    if len(rptname) == 0:
       filename = "output" + ".csv"
    else :         
       filename = rptname + ".csv"
    st.download_button(
        "Download file",
        csv,
        filename,
        "text/csv",
        key='download-csv'
    )        

def convert_df_excel(df: pd.DataFrame, filenm = "" ) :
    return df.to_excel()

def convert_df(df: pd.DataFrame):
        
        return df.to_csv(index=False).encode('utf-8')


def lopinoperation() :

   login = Login()

   result = False

   with st.container() :

        buff, col, col2, buff2 = st.columns([1,2,2,1])

    
        col.subheader("Login")

        username = col.text_input("User Name", key= "user").strip()
        password = col.text_input("Password",type='password', key= "pass").strip()

        col1, col2 = col.columns([2,1])
        login_btn = col1.button("Sign-In")

   if login_btn :

        if len(str(username).strip()) == 0 :
           col.error ("Please enter user name")
           st.stop()
        elif  len(str(password).strip()) == 0 :
           col.error ("Please enter password")
           st.stop()
        else :
            if len(str(username).strip()) > 0 and len(str(password).strip()) > 0 :
                result =login.SignIn(username,password)
        if result:
                col.success("Logged In as {}".format(username))
                st.session_state.is_loggedin = True
                st.session_state.user_name = str(username)
                st.session_state.user_id_rnd = login.getuserid(username)
        else :
                col.error("Login credential does not exist.Please sign-Up")

with st.sidebar.container():
        
        selected = option_menu("", ["Sign-In", "Sign-Up", "Allocation upload" , "Report", 'Logout'], 
        icons=['person', 'door-open', 'cloud-upload' , 'bar-chart', 'door-closed'], 

        menu_icon="cast", default_index=0 
        # styles={
        #      "container": {"padding": "0!important", "background-color": "#eee"},
        #      "icon": {"color": "orange", "font-size": "25px"}, 
        #      "nav-link": {"font-size": "15px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
        #      "nav-link-selected": {"background-color": "blue"},
        #  }
        
        )



if selected == "Sign-In" :
       
       lopinoperation()
        

if selected == "Sign-Up" :
    SignUp()
    

def IsTransactionExists(s_path: str, s_pl = "PL.parquet", s_tr  = "TrancheData.parquet") -> bool:
    pl_file = s_path + "//" + s_pl
    tr_file = s_path + "//" + s_tr
    exist = os.path.isfile(pl_file) and os.path.isfile(tr_file)
    return exist


def filterDataActivity(df : pd.DataFrame, isdetail = 1) :
    chk_all = st.sidebar.toggle(label= "All", value= False)
    
    if isdetail == 1:
       
       allocclasses =  sorted(df["AllocationClass"].unique())
       df_na = df["LedgerAccount"].dropna(how='all').to_frame()
       
       ledgerAccount =  df_na["LedgerAccount"].unique()

       with st.expander("Ledger Account"):
            all = st.toggle(label= "All",value= True, key= "all")
            if all :
                select_ledgeraccount = st.multiselect(label= "Choose Ledger Account", options= ledgerAccount, default=ledgerAccount) 
            else :    
                select_ledgeraccount = st.multiselect(label= "Choose Ledger Account", options= ledgerAccount, default=None)    

       with st.container():
        if chk_all :
            
                    Select_allocclasses = st.sidebar.multiselect(label= "Choose Alloc Class", options= allocclasses, default=allocclasses)
        else :
                    Select_allocclasses = st.sidebar.multiselect(label= "Choose Alloc Class", options= allocclasses, default=None) 
            

       filter_ledgeraccount = df["LedgerAccount"].isin(select_ledgeraccount)
       filter_Allocclass = df["AllocationClass"].isin(Select_allocclasses)
       df_filterd = df[filter_ledgeraccount & filter_Allocclass]
       

    elif isdetail == 0:
       allocclasses =  sorted(df["AllocationClass"].unique())
       if chk_all :
        
            Select_allocclasses = st.sidebar.multiselect(label= "Choose AllocClass", options= allocclasses, default=allocclasses)
       else :
            Select_allocclasses = st.sidebar.multiselect(label= "Choose AllocClass", options= allocclasses, default=None) 

       filter_Allocclass = df["AllocationClass"].isin(Select_allocclasses)
       df_filterd = df[filter_Allocclass]

    return df_filterd    

def preparereport(s_upload: Upload, selected_outputpath : str, selected_report : str):
    s_calcfolder = s_upload.basefolder() + "//" + selected_outputpath + "//" + s_upload.calcforder + "//"
    parquet_path_calc = Path(s_calcfolder)
    
    if os.path.isdir(parquet_path_calc) :
        for i, parquet_path_calc in enumerate(parquet_path_calc.glob('*.parquet')):
            
            if i == 0 :
                df_m = s_upload.ReadParquetFile(parquet_path_calc)
                st.session_state.df_m = df_m
            elif i == 1 :
                df_on = s_upload.ReadParquetFile(parquet_path_calc)
                st.session_state.df_on = df_on
            elif i == 2 :        
                df_off = s_upload.ReadParquetFile(parquet_path_calc)
                st.session_state.df_off = df_off
            else :
                pass     

        if st.session_state.df_m is not None and st.session_state.df_on is not None and st.session_state.df_off is not None :
            file_path = s_upload.basefolder() + selected_outputpath + "\\"
            
            alloc_rpt = Allocation(st.session_state.TranchDFALL, st.session_state.PLDFALL,file_path )
              
             
            if selected_report == "Combined Detail":
                
                chk_pivot = st.toggle("Transpose Ledger Account", value= False)
                if not chk_pivot :
                    df = alloc_rpt.CombinedDetail(st.session_state.df_m, st.session_state.df_on, st.session_state.df_off)
                    df = filterDataActivity(df)
                
                    st.data_editor(df.head(100), key= "1", use_container_width=True, hide_index= True)
                else:     
                 
                    types_options = ["ALL", "Master", "Onshore" , "Offshore" , "Total" ]
                    select_type = st.selectbox("Choose Type", options= types_options)
                    df = alloc_rpt.CombinedDetailPivot(st.session_state.df_m, st.session_state.df_on, st.session_state.df_off, select_type)
                    df = filterDataActivity(df,0)
                
                    st.data_editor(df, key= "5", use_container_width=True, hide_index= True)

            if selected_report == "Combined Summary":
            
                df = alloc_rpt.CombinedSummary(st.session_state.df_m, st.session_state.df_on, st.session_state.df_off)
                df = filterDataActivity(df,0)
                #df = st.session_state.df_m
                st.data_editor(df.head(100), key= "2", use_container_width=True, hide_index= True)

            if selected_report == "Combined PL":
            
                df = alloc_rpt.CombinedPL(st.session_state.df_m, st.session_state.df_on, st.session_state.df_off)
                df = filterDataActivity(df)
                #df = st.session_state.df_m
                st.data_editor(df.head(100), key= "3", use_container_width=True, hide_index= True)             

            if selected_report == "Combined before adjustement" :

               df = alloc_rpt.CombinedDetailActual(st.session_state.df_m, st.session_state.df_on, st.session_state.df_off)
               df = filterDataActivity(df)
                
               st.data_editor(df.head(100), key= "4", use_container_width=True, hide_index= True) 

            download_csv(df, selected_report)

    else :
        st.error("Please run calculation for "  + str(selected_outputpath))
        st.stop()



if selected == "Report" :
   if not st.session_state.is_loggedin :
      st.error ("Please Sign-in ")
      st.stop()    
   else :
    col1 , col2 = st.columns([.3, .7])
    reports = ["Combined Detail", "Combined Summary" , "Combined PL" , "Combined before adjustement"]
    upd_rpt = Upload("",st.session_state.user_name,"","","CALC")
    s_base_folder =  upd_rpt.basefolder()
    sub_folders = [name for name in os.listdir(s_base_folder) if os.path.isdir(os.path.join(s_base_folder,name))]
    output_path = col1.selectbox("SELECT FUND", options= sub_folders,placeholder= "Choose a Fund")
        
    report_list = col2.selectbox("REPORTS", options=reports)
    preparereport(upd_rpt,output_path,report_list)

if selected == "Allocation upload" :
   if not st.session_state.is_loggedin :
      st.error ("Please Sign-in ")
      st.stop()    
   else :
       datetime_now = datetime.now()
       format = '%Y-%m'

       dtst_name = datetime_now.strftime(format)

       col1, col2 = st.columns([1,4])        
       file_p = st.file_uploader(label= "Upload File for Allocation (EXCEL)",type=['XLSX'] , accept_multiple_files= True) 
       
       datasetname = st.text_input(label="FUND", key= "dtst", value= str(dtst_name)).strip()
       colx, coly = st.columns([.5,.5])
       col1 , col2, col3, = colx.columns([.3, .3, .4],gap='small')
       b_prcs =  col1.button("Upload")
       b_show = col2.button("Display")
       b_alloc = col3.button("Run Allocation")
       types = ["Master", "Onshore", "Offshore"]
       m_fundtypes  = st.multiselect("ALLOCATION TYPE", options= types , default=types, disabled= True)

       if b_prcs or b_show or b_alloc :
          
          if len(datasetname) == 0:
             st.error("Fund cannot be blank") 
             st.stop()

       if file_p and b_prcs :
            
            with st.spinner("Uploading  data .Please wait...") :
                up = Upload(file_p,st.session_state.user_name,GetCurrentdate(),customfilename= datasetname)
                up.xlsx_to_parquet()
                st.success("Data uploaded. Please run Allocation")

        
       upd = Upload("",st.session_state.user_name,GetCurrentdate(),customfilename= datasetname)
       path_pq = upd.GetParquetFolder(datasetname)
       parquet_path = Path(path_pq)
         
       pq_file_name = []
       ext = ".parquet" 

       for files in os.listdir(path_pq) :

       #for i, parquet_path in enumerate(parquet_path.glob('*.parquet')):
                if files.endswith(ext) :         
                    
                    parquet_file_path =  path_pq + "\\" + files
                    df = upd.ReadParquetFile(parquet_file_path)
                    st.write(parquet_file_path)
                    data_set = sorted(df["sheet_name"].unique())
                    if data_set[0] == "PL" :
                    #st.session_state.PLDF = df
                        st.session_state.PLDFALL = df
                    else :
                    #st.session_state.TranchDF = df
                        st.session_state.TranchDFALL = df
                    #pq_file_name.append(data_set)
                    pq_file_name.extend(data_set)

       def PrepareAllocatedData(df_t : pd.DataFrame, df_pl : pd.DataFrame, file_name : str, path_pq = ""):
           alloc = Allocation(df_t,df_pl,path_pq)
           df = alloc.PLAllocation()
           upd_calc = Upload("",st.session_state.user_name,"","","CALC")
           upd_calc.uploadcalculatedAllocation(df,datasetname,file_name)
           

       selected_data = st.multiselect("DATA SETS", options=pq_file_name, default= pq_file_name, disabled= True)
         
       if b_show :
        for i in selected_data :
            if i == "PL" :
                st.data_editor(st.session_state.PLDFALL, key= str(i), use_container_width= True , hide_index= True)
            elif i == "TrancheData" :
                st.data_editor(st.session_state.TranchDFALL, key= str(i), use_container_width= True , hide_index= True)        
            else :
                pass
                
       if m_fundtypes is not None and st.session_state.TranchDFALL is not None and st.session_state.PLDFALL is not None :
           
           alloctype = Allocation(st.session_state.TranchDFALL,st.session_state.PLDFALL,path_pq)
           
           if b_alloc :
              if not IsTransactionExists(path_pq):
                 st.error("Upload the files in the path")
                 st.stop() 

            #   st.data_editor(alloctype.PLAllocation())
            #   st.stop()
            #st.spinner("Allocation in progress..")

              for i in range(3) :
                if i == 0 :
                    #df = alloctype.TrancheDataMaster()
                    st.session_state.TranchDF = st.session_state.TranchDFALL
                    #st.session_state.TranchDF = df
                    st.session_state.PLDF = st.session_state.PLDFALL
                    PrepareAllocatedData(st.session_state.TranchDF,st.session_state.PLDF,str(i),path_pq) 

                elif i == 1 :
                    df = alloctype.TrancheDataOnshore()
                    df_pl = alloctype.PlDataOnshore()
                    st.session_state.TranchDF = df
                    st.session_state.PLDF = df_pl 
                    PrepareAllocatedData(st.session_state.TranchDF,st.session_state.PLDF,str(i),path_pq) 
                elif i == 2 :
                    df = alloctype.TrancheDataOffshore()
                    df_pl = alloctype.PlDataOffshore()
                    st.session_state.TranchDF = df
                    st.session_state.PLDF = df_pl 
                    PrepareAllocatedData(st.session_state.TranchDF,st.session_state.PLDF,str(i),path_pq)  
                else :
                    pass
              st.success("Allocation completed!")        
    
       
       
if selected == "Logout" :
   if st.session_state.is_loggedin :
        st.subheader(f"User  {st.session_state.user_name}  is successfully logged out")
        st.session_state.clear()
   else :
        st.error ("Please Sign-in")
        st.stop()
     
