# -*- coding: utf-8 -*-
"""
Modulo de conexion a la BD.
Implemento la BD como una clase
En el init me conecto y me quedo con el conector.
Luego, c/metodo me da los parametros que quiera
"""
from spy_bd_bdspy import BDSPY
from spy_bd_gda import BDGDA, BDGDA_TAHONA
from spy_bd_ose import BDOSE_PQ, BDOSE_PZ, BDOSE_TQ
from spy_log import log
#------------------------------------------------------------------------------
class BD:
    
    def __init__ (self, dlgid, modo = 'local'):
        self.modo = modo
        self.dlgid = dlgid
        self.bdr = ''

        bd = BDSPY(self.modo)
        self.datasource = bd.find_data_source(self.dlgid)
        if self.datasource == 'GDA':
            self.bdr = BDGDA(self.modo)
        elif self.datasource == 'GDA_TAHONA':
            self.bdr = BDGDA_TAHONA(self.modo)
        elif self.datasource == 'BDOSE_PQ':
            self.bdr = BDOSE_PQ(self.modo)
        elif self.datasource == 'BDOSE_PZ':
            self.bdr = BDOSE_PZ(self.modo)
        elif self.datasource == 'BDOSE_TQ':
            self.bdr = BDOSE_TQ(self.modo)
        else:
            log(module=__name__, function='__init__', level='INFO', dlgid=dlgid, msg='DS={} NOT implemented'.format(self.datasource))

        return


    def process_commited_conf(self):

        log(module=__name__, function='process_commited_conf', level='SELECT', dlgid=self.dlgid, msg='DS={}'.format(self.datasource))
        cc = self.bdr.process_commited_conf(self.dlgid)
        return cc


    def clear_commited_conf(self):
        '''
        Reseteo el valor de commited_conf a 0
        '''
        log(module=__name__, function='clear_commited_conf', level='SELECT', dlgid=self.dlgid, msg='DS={}'.format(self.datasource))
        self.bdr.clear_commited_conf(self.dlgid)
        return


    def find_datasource( self, dlgid ):
        # Si ya tengo el datasource no tengo que consultar a la BD.
        if self.datasource != '':
            return

        bd = BDSPY(self.modo)
        self.datasource = bd.find_data_source(dlgid)
        return


    def reset_datasource(self, dlgid):
        self.datasource = ''
        log(module=__name__, function='reset_datasource', level='SELECT', dlgid=dlgid, msg='start')


    def read_dlg_conf( self ):
        '''
        Leo toda la configuracion de la BD que hay del dlg dado.
        El dlg puede estar en cualquier BD ( gda, bd_ose ) y con esta funcion abstraigo al resto
        del programa de donde esta.
        Retorno un diccionario con un indice doble ( canal, parametro )
        '''
        log(module=__name__, function='read_dlg_conf', level='SELECT', dlgid=self.dlgid, msg='start')
        # En ppio no se en que BD esta definido el datalogger. Debo leer su datasource
        d = self.bdr.read_dlg_conf(self.dlgid)
        return d


    def read_piloto_conf(self ):
        log(module=__name__, function='read_piloto_conf', level='SELECT', dlgid=self.dlgid, msg='start')
        # En ppio no se en que BD esta definido el datalogger. Debo leer su datasource
        d = self.bdr.read_piloto_conf(self.dlgid)
        return d


    def update(self, d):
        '''
        Recibo un diccionario y a partir de este genero los inserts en la BD.
        Determino en que BD debo trabajar.

        '''
        self.bdr.update(self.dlgid, d)
        return


    def insert_data_line(self,d):
        self.bdr.insert_data_line(self.dlgid, d)
        return


    def insert_data_online(self,d):
        self.bdr.insert_data_online(self.dlgid, d)
        return