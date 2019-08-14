#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 14:37:29 2019

@author: pablo

Importar el modulo redis a python
pip3 install redis

"""

import redis
import logging
# Creo un logger local child.
LOG = logging.getLogger(__name__)

#------------------------------------------------------------------------------

class Redis():
    
    
    def __init__(self, dlgid):
        '''
        Me conecto a la redis local y dejo el conector en un objeto local
        '''
        self.dlgid = dlgid
        self.connected = ''
        self.rh = ''
        try:
            self.rh = redis.Redis()
            self.connected = True
        except:
            LOG.info('[%s] Redis init ERROR !!' % self.dlgid )
            self.connected = False

    
    def create_rcd(self):
        if self.connected:
            self.rh.hset( self.dlgid, 'LINE', 'NUL')
            self.rh.hset( self.dlgid, 'OUTPUTS', '-1')
            self.rh.hset( self.dlgid, 'RESET', 'FALSE')
            self.rh.hset( self.dlgid, 'POUT', '-1')
            self.rh.hset( self.dlgid, 'PSLOT', '-1')
            self.rh.hset( self.dlgid, 'MEMFORMAT', 'FALSE')
            LOG.info('[%s] Redis init rcd. OK !!' % self.dlgid)
        else:
            LOG.info('[%s] Redis not-connected (create_rcd) !!' % self.dlgid)

            
    def insert_line(self, line):
        '''
        Inserto la ultima linea de datos en la redis
        '''
        if self.connected:
            try:
                self.rh.hset( self.dlgid, 'LINE', line )
            except:  
                LOG.info('[%s] Redis insert line ERROR !!' % self.dlgid)
        else:
             LOG.info('[%s] Redis not-connected (insert_line) !!' % self.dlgid)
        return
 
    
    def get_cmd_outputs(self):
        # SALIDAS
        response = ''
        if self.connected:
            if self.rh.hexists(self.dlgid, 'OUTPUTS'):
                outputs = int(self.rh.hget( self.dlgid, 'OUTPUTS' ))
                if outputs != -1:
                    response = 'DOUTS=%s:' % outputs
        
            self.rh.hset( self.dlgid, 'OUTPUTS', '-1' )
        else:
             LOG.info('[%s] Redis not-connected (get_cmd_outputs) !!' % self.dlgid)

        return(response)
 
       
    def get_cmd_pilotos(self):
        # PILOTO
        response = ''
        if self.connected:
            if self.rh.hexists(self.dlgid, 'POUT'):
                pslot = int(self.rh.hget( self.dlgid, 'PSLOT'))
                pout = float(self.rh.hget( self.dlgid, 'POUT'))
                if pout != -1:
                    response += 'POUT=%s,%s:' % ( pslot, pout)
            
            self.rh.hset( self.dlgid, 'POUT', '-1')
            self.rh.hset( self.dlgid, 'PSLOT', '-1')    
        else:
             LOG.info('[%s] Redis not-connected (get_cmd_pilotos) !!' % self.dlgid)           
 
        return(response)


    def get_cmd_reset(self):        
        # RESET
        response = ''
        if self.connected:
            if self.rh.hexists(self.dlgid, 'RESET'):
                r = self.rh.hget( self.dlgid, 'RESET' )
                # r es un bytes b'RESET'
                # Para usarlo en comparaciones con strings debo convertirlos a str con decode()
                reset = r.decode()
                # Ahora reset es un str NO un booleano por lo tanto comparo contra 'True'
                if reset == 'True':
                    response += 'RESET:'
                
                self.rh.hset( self.dlgid, 'RESET', 'False' )        
        else:
             LOG.info('[%s] Redis not-connected (get_cmd_reset) !!' % self.dlgid)    
        
        return(response)
        
