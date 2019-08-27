#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 14:37:29 2019

@author: pablo

Importar el modulo redis a python
pip3 install redis

"""

import redis
from spy_log import log

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
        except Exception as err_var:
            log(module=__name__, function='__init__', dlgid=self.dlgid, msg='Redis init ERROR !!')
            log(module=__name__, function='__init__', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
            self.connected = False

    
    def create_rcd(self):
        if self.connected:
            if not self.rh.hexists(self.dlgid, 'LINE'):
                self.rh.hset( self.dlgid, 'LINE', 'NUL')
            if not self.rh.hexists(self.dlgid, 'OUTPUTS'):
                self.rh.hset( self.dlgid, 'OUTPUTS', '-1')
            if not self.rh.hexists(self.dlgid, 'RESET'):
                self.rh.hset( self.dlgid, 'RESET', 'FALSE')
            if not self.rh.hexists(self.dlgid, 'POUT'):
                self.rh.hset( self.dlgid, 'POUT', '-1')
            if not self.rh.hexists(self.dlgid, 'PSLOT'):
                self.rh.hset( self.dlgid, 'PSLOT', '-1')
            if not self.rh.hexists(self.dlgid, 'MEMFORMAT'):
                self.rh.hset( self.dlgid, 'MEMFORMAT', 'FALSE')

            log(module=__name__, function='create_rcd', level='SELECT', dlgid=self.dlgid, msg='Redis init rcd. OK !!')
        else:
            log(module=__name__, function='create_rcd', dlgid=self.dlgid, msg='Redis not-connected !!')


    def insert_line(self, line):
        '''
        Inserto la ultima linea de datos en la redis
        Debo agregar un TAG de la forma 100&LINE= para que los scripts de Yosniel puedan parsearlo
        '''
        TAG = 'LINE='
        line = TAG + line
        if self.connected:
            try:
                self.rh.hset( self.dlgid, 'LINE', line )
            except Exception as err_var:
                log(module=__name__, function='insert_line', dlgid=self.dlgid, msg='Redis insert line ERROR !!')
                log(module=__name__, function='insert_line', dlgid=self.dlgid, msg='EXCEPTION {}'.format(err_var))
        else:
            log(module=__name__, function='insert_line', dlgid=self.dlgid, msg='Redis not-connected !!')
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
            log(module=__name__, function='get_cmd_outputs', dlgid=self.dlgid, msg='Redis not-connected !!')

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
            log(module=__name__, function='get_cmd_pilotos', dlgid=self.dlgid, msg='Redis not-connected !!')
 
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
                if reset.upper() == 'TRUE':
                    response += 'RESET:'
                
                self.rh.hset( self.dlgid, 'RESET', 'FALSE' )
        else:
            log(module=__name__, function='get_cmd_reset', dlgid=self.dlgid, msg='Redis not-connected !!')
        
        return(response)
        

if __name__ == '__main__':
    print('Testing Redis module')
    rd = Redis('PRUEBA')
    rd.create_rcd()
    reset = rd.rh.hget('PRUEBA','RESET')
    print('Reset INIT=%s' % reset)

    rd.rh.hset('PRUEBA', 'RESET', 'False')
    reset = rd.rh.hget('PRUEBA','RESET')
    print('Reset Rd=%s' % reset)

    if rd.rh.exists('PRUEBA2'):
        print('Prueba 2 existe')
    else:
        print ('No existe')
    response = rd.get_cmd_reset()
    print ('RSP=%s' % response)

