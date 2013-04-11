# -*- coding: utf-8 -*-
##############################################################################
#
#    Copyright (C) 2013 Enapps LTD (<http://www.enapps.co.uk>).
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

from osv import osv, fields
import MySQLdb as mdb


class mysql_config(osv.osv):
    _name = 'mysql.config'
    _rec_name = 'host'

    _columns = {
        'name': fields.char('Name', size=512),
        'host': fields.char('Host', size=512, required=True),
        'port': fields.integer('Port'),
        'username': fields.char('Username', size=512, required=True),
        'passwd': fields.char('Password', size=512, required=True),
        'db': fields.char('Database', size=512, required=True),
        'query': fields.text('Query'),
    }

    _defaults = {
        'host': 'localhost',
        'port': 3306,
    }

    def test_connection(self, cr, uid, ids, context={}):
        for config in self.browse(cr, uid, ids, context=context):
            try:
                connection = mdb.connect(user=config.username, host=config.host, port=config.port, passwd=config.passwd, db=config.db)
                connection.close()
            except:
                raise osv.except_osv(('Error !'), ("Can't establish connection to %s! Please, check settings." % config.host))
            raise osv.except_osv(('Success!'), ("Connection to %s established!" % config.host))

mysql_config()
# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
