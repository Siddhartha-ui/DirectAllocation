import hashlib
import duckdb as db
import pandas as pd
import random

class Login :
	
    def __init__(self) -> None:
        conn = db.connect('UserDB\\user.db')
        random_number = random.randint(100000, 999999) 
        self.__con = conn
        self.__randomnum = random_number

    @property
    def con(self):
        return self.__con.cursor()

    @property
    def randomnum(self) -> int:
        return self.__randomnum

    def __create_usertable(self):
        self.con.execute(f"""CREATE  TABLE IF NOT EXISTS userstable(username TEXT,password TEXT , uniquerndno int)""")    

    def create_usertable(self): 
        return self.__create_usertable()

    def add_userdata(self,username : str,password : str):
        __randomUserId = self.__randomnum
        self.con.execute(f"""INSERT INTO userstable(username,password,uniquerndno) VALUES (?,?,?)""",(username,password,__randomUserId))
        self.con.commit()
    
    def __login_user(self,username : str,password : str) -> bool :
            username = f"'{username}'"
            password = f"'{password}'"
            
            df = self.con.execute(f'''
                SELECT * FROM userstable WHERE username = {username} AND password = {password}
            ''').df()
            
            return not df.empty
            

    def make_hashes(self,password : str) :
        return self.__make_hashes(password=password)

    def __make_hashes(self,password : str):
        return hashlib.sha256(str.encode(password)).hexdigest()

    def __check_hashes(self ,password : str,hashed_text : str) -> str:
        if self.__make_hashes(password) == hashed_text:
            return hashed_text
        return ''

    def ValidateConfirmPass(self,password : str, confirm_password : str) -> bool :
        return (password == confirm_password)

    def PasswordRule(self, password : str, passlen : int) -> str:
        
        if len(password.strip()) < passlen :
           ret = f"""Password length cannot be less than {passlen} characters"""
        else :
           ret = ""     
        return ret   

    def CheckDuplicateuser(self,username : str ) -> bool :
            
        username = f"'{username}'"
                
        df = self.con.execute(f'''
                    SELECT * FROM userstable WHERE username = {username} 
        ''').df()
                
        return df.empty

    def getuserid(self,username : str) -> str:
        username = f"'{username}'"
        df = self.con.execute(f"""
                select uniquerndno from userstable WHERE username = {username} 

            """
        ).df()

        return str(df.iloc[0]['uniquerndno'])

    def view_all_users(self) -> pd.DataFrame:
        df = self.con.sql('SELECT * FROM userstable').df()
        return df

    def SignIn(self,username : str , password : str) -> bool :
        self.__create_usertable()
        hashed_pswd = self.__make_hashes(password)
        result = self.__login_user(username,self.__check_hashes(password,hashed_pswd))
        return result
