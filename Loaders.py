import time
import json
import datetime
import random
import math
import configparser
import _thread
from collections import deque

import boto3
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tqdm import tqdm

from IPython.display import display, Markdown, clear_output
import ipywidgets as widgets

class SQLLoader:
    """
    
    """
    AVAILABLE = "AVAILABLE"
    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    DESTROYED = "DESTOYED"
    
    def __init__(self, file, dbengine, dbhost, dbuser, dbpass, dbname, dbtable, drop=False, dtype={}, date_fields=[]):
        """
        
        """
        self.file = file
        self.engine = create_engine(f'{dbengine}://{dbuser}:{dbpass}@{dbhost}:5432/{dbname}')
        self.dbtable = dbtable
        
        self.iteration = 0
        self.registers_inserted = 0
        self.registers_updated = 0
        self.registers_deleted = 0
        
        self.__status = {
            "state": self.AVAILABLE,
            "iteration":self.iteration,
            "inserted":self.registers_inserted,
            "updated":self.registers_updated,
            "deleted":self.registers_deleted,
            "indb":0
        }
        
        self.df = pd.read_csv(f"{self.file}",dtype=dtype, parse_dates=date_fields)
        self.df["_insert"] = 0
        self.df["_insert_time"] = 0
        self.df["_update"] = 0
        self.df["_update_time"] = 0
        self.df["_delete"] = 0
        self.df["_delete_time"] = 0
        
        if drop:
            self.drop_table() 
    
        # -----------
        # UI ELEMENTS
        # 
        self.out = widgets.Output()
        start_button = widgets.Button(description='Start')
        stop_button = widgets.Button(description='Stop')
        destroy_button = widgets.Button(description='Destroy Process')
        
        def start_button_clicked(_):
            self.__status["state"] = self.RUNNING
            
        def stop_button_clicked(_):
            self.__status["state"] = self.STOPPED
            
        def destroy_button_clicked(_):
            self.__status["state"] = self.DESTROYED
            
        start_button.on_click(start_button_clicked)
        stop_button.on_click(stop_button_clicked)
        destroy_button.on_click(destroy_button_clicked)
        
        self.buttons = widgets.HBox([start_button, stop_button, destroy_button])    
        self.box = widgets.VBox([self.buttons, self.out])
    
    def insert(self, registers=10, delay=0):
        """
        
        """
        temp_df = self.df.iloc[self.registers_inserted : self.registers_inserted + registers].copy()
        temp_df["_insert"] = 1        
        temp_df["_insert_time"] = time.time()
        temp_df.drop(["_delete","_delete_time"],axis=1).to_sql(
            name=self.dbtable,
            con=self.engine,
            if_exists="append"
        )
        self.df.iloc[self.registers_inserted : self.registers_inserted + registers] = temp_df
        self.registers_inserted += registers
        time.sleep(delay)
    
    def update(self, registers=5, delay=0):
        """
        
        """
        if registers > 0:
            temp_df = self.df.iloc[:self.registers_inserted][self.df._delete==0].sample(n=registers).copy()
            temp_df["_update"] = temp_df["_update"] + 1
            temp_df["_update_time"] = time.time()

            indexes_str = map(lambda x: str(x), temp_df.index.to_list())
            indexes = ",".join(indexes_str)
            self.engine.execute(f"""
                UPDATE {self.dbtable}
                SET _update = _update + 1, _update_time = {time.time()}
                WHERE index IN ({indexes})
            """)
            self.df[self.df.index.isin(temp_df.index)] = temp_df.copy()
            self.registers_updated += len(temp_df)
            time.sleep(delay)
    
    def delete(self, registers=1, delay=0):
        """
        
        """
        if registers > 0:
            temp_df = self.df.iloc[:self.registers_inserted][self.df._delete==0].sample(n=registers).copy()
            temp_df["_delete"] = temp_df["_delete"] + 1
            temp_df["_delete_time"] = time.time()

            indexes_str = map(lambda x: str(x), temp_df.index.to_list())
            indexes = ",".join(indexes_str)
            self.engine.execute(f"""
                DELETE FROM {self.dbtable}
                WHERE index IN ({indexes})
            """)
            self.df[self.df.index.isin(temp_df.index)] = temp_df.copy()
            self.registers_deleted += len(temp_df)
            time.sleep(delay)

    def iud(self, inserts=10, updates=5, deletes=0, delay=0, max_registers=None, uix=False):
        """
        
        """     
        assert inserts > 0, "'inserts' must be grather than 0."
        assert inserts > deletes, "'inserts' must be grather than 'deletes'."
        
        self.__status["state"] = self.RUNNING
        num_to_insert = len(self.df[self.df["_insert"]==0])
        if max_registers:
            num_iters = math.ceil(max_registers/float(inserts)) if num_to_insert > max_registers else math.ceil(num_to_insert/float(inserts))
        else:
            num_iters = math.ceil(num_to_insert/inserts)
        iterations = range(num_iters) if uix else tqdm(range(num_iters))
        message = ""
        
        for i in iterations:
            start = time.time()
            
            # --------
            # Insert
            self.insert(inserts)
            
            # --------
            # Update
            self.update(updates)
            
            # --------
            # Delete
            self.delete(deletes)
            
            # --------
            # Delay
            st = self.status()
            time.sleep(delay)
            
            # --------
            # Stats
            in_db = st["indb"]
            inserted = st["inserted"]
            updated = st["updated"]
            deleted = st["deleted"]
            self.iteration +=1
            delta = float(time.time() - start)
            state = self.__status["state"]
            i_s = inserts/delta
            u_s = updates/delta
            d_s = deletes/delta
            percentage = 100.0*i/float(num_iters)
            play = "▶" if i%2==0 else "·"
            
            message = f"{play}[{state}] DB:{in_db} | I:{inserted}, U:{updated}, D:{deleted} | I/s:{i_s:.0f}, U/s:{u_s:.0f}, D/s:{d_s:.0f} | {percentage:.2f}%"            
            
            # --------
            # UI
            if uix==False:
                iterations.set_description(message)
            else:
                with self.out:
                    clear_output(wait=True)
                    display(message)
                
                while self.__status["state"] == self.STOPPED:
                    time.sleep(0.01)
                
                # --------
                #
                if self.__status["state"] == self.DESTROYED:
                    message = f"[{state}] DB:{in_db} | I:{inserted}, U:{updated}, D:{deleted} | I/s:{i_s:.0f}, U/s:{u_s:.0f}, D/s:{d_s:.0f} | {percentage:.2f}%"
                    with self.out:
                        clear_output(wait=True)
                        display(message)
                    break
                    
        self.__status["state"] = self.AVAILABLE
        message = message.replace(self.RUNNING, self.AVAILABLE)
        clear_output(wait=True)
        print(message)
    
    def iudx(self, inserts=10, updates=5, deletes=0, delay=0, max_registers = None):
        """
        
        """
        if self.__status["state"] == self.RUNNING or self.__status["state"] == self.STOPPED:
            print("There is already a 'iudx' load in progress")
        else:
            _thread.start_new_thread(self.iud, (inserts, updates , deletes, delay, max_registers, True))
        return self.box

    def status(self):
        """
        
        """
        registers_in_db = None
        try:
            registers_in_db = list(self.engine.execute(f'SELECT COUNT(*) FROM {self.dbtable}'))[0][0]
        except:
            pass
        
        self.__status["iteration"] = self.iteration
        self.__status["inserted"] = self.registers_inserted
        self.__status["updated"] = self.registers_updated
        self.__status["deleted"] = self.registers_deleted
        self.__status["indb"] = registers_in_db
        return self.__status
        
    def drop_table(self):
        """
        
        """
        self.iteration = 0
        self.registers_inserted = 0
        self.registers_updated = 0
        self.registers_deleted = 0
        
        try:
            self.engine.execute(f'SELECT COUNT(*) FROM {self.dbtable}')
            self.engine.execute(f'DROP TABLE {self.dbtable}')
        except:
            pass