import streamlit as st

class Session(object):
    def __init__(self) -> None:
           
        if "user_name" not in st.session_state :
            st.session_state.user_name = "" 
        if "sign_up" not in st.session_state :
            st.session_state.sign_up = False
        if "is_loggedin" not in st.session_state :
            st.session_state.is_loggedin = False
        
        if "TranchDF" not in st.session_state :
                st.session_state.TranchDF = None

        if "TranchDFALL" not in st.session_state :
                st.session_state.TranchDFALL = None

        if "PLDF" not in st.session_state :
                st.session_state.PLDF = None

        if "PLDFALL" not in st.session_state :
                st.session_state.PLDFALL = None    

        if "df" not in st.session_state :
                st.session_state.df = None

        if "df_m" not in st.session_state :
                st.session_state.df_m = None

        if "df_on" not in st.session_state :
                st.session_state.df_on = None    

        if "df_off" not in st.session_state :
                st.session_state.df_off = None
                
        if "user_id_rnd" not in st.session_state :
            st.session_state.user_id_rnd = ""         