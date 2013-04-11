# -*- coding: utf-8 -*-
##############################################################################
#
#    Copyright (C) 2011 Enapps LTD (<http://www.enapps.co.uk>).
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

from osv import osv
from osv import fields
import base64
import csv
import re
import MySQLdb as mdb
import datetime
from cStringIO import StringIO


def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, charset='utf-8', **kwargs):
    # csv.py doesn't do Unicode; encode temporarily as UTF-8:
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data, charset),
                            dialect=dialect, **kwargs)
    for row in csv_reader:
        # decode UTF-8 back to Unicode, cell by cell:
        yield [unicode(cell, charset) for cell in row]


def utf_8_encoder(unicode_csv_data, charset):
    for line in unicode_csv_data:
        yield line
        #yield line.decode(charset).encode('utf-8', 'ignore')


class ea_import_chain(osv.osv):
    _name = 'ea_import.chain'
    _columns = {
        'name': fields.char('Name', size=256, required=True),
        'type': fields.selection([
            ('csv', 'CSV Import'),
            ('mysql', 'MySql Import'),
        ], 'Import Type', required=True, help="Type of connection import will be done from"),
        'mysql_config_id': fields.many2one('mysql.config', 'MySql configuration'),
        'input_file': fields.binary('Test Importing File', required=False),
        'header': fields.boolean('Header'),
        'link_ids': fields.one2many('ea_import.chain.link', 'chain_id', 'Chain Links', ),
        'result_ids': fields.one2many('ea_import.chain.result', 'chain_id', 'Import Results', ),  # to be removed
        'log_ids': fields.one2many('ea_import.log', 'chain_id', 'Import Log', ),
        'separator': fields.selection([
            (",", '<,> - Coma'),
            (";", '<;> - Semicolon'),
            ("\t", '<TAB> - Tab'),
            (" ", '<SPACE> - Space'),
        ], 'Separator', required=True),
        'delimiter': fields.selection([
            ("'", '<\'> - Single quotation mark'),
            ('"', '<"> - Double quotation mark'),
            ('None', 'None'),
        ], 'Delimiter', ),
        'charset': fields.selection([
            ('us-ascii', 'US-ASCII'),
            ('utf-7', 'Unicode (UTF-7)'),
            ('utf-8', 'Unicode (UTF-8)'),
            ('utf-16', 'Unicode (UTF-16)'),
            ('windows-1250', 'Central European (Windows 1252)'),
            ('windows-1251', 'Cyrillic (Windows 1251)'),
            ('iso-8859-1', 'Western European (ISO)'),
            ('iso-8859-15', 'Latin 9 (ISO)'),
        ], 'Encoding', required=True),
        'model_id': fields.many2one('ir.model', 'Related document model'),
        'ir_act_window_id': fields.many2one('ir.actions.act_window', 'Sidebar action', readonly=True, ),
        'ir_value_id': fields.many2one('ir.values', 'Sidebar button', readonly=True, ),
        }

    _defaults = {
        'separator': ",",
        'charset': 'utf-8',
        'delimiter': None,
        'type': 'csv',
    }

    def get_mysql_data(self, config_obj):
        connect = mdb.connect(host=config_obj.host, user=config_obj.username,
                           passwd=config_obj.passwd, db=config_obj.db)
        connect.escape_string("'")
        cursor = connect.cursor()
        if re.match(r'CREATE|DROP|UPDATE|DELETE', config_obj.query, re.IGNORECASE):
            raise osv.except_osv(('Error !'), ("Operation prohibitet!"))
        cursor.execute(config_obj.query)
        data = cursor.fetchall()
        connect.close()
        return '\n'.join([row[1:-2] for row in [str(item) + ',' for item in data]])

    def import_to_db(self, cr, uid, ids, context={}):
        time_of_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        context.update({'time_of_start': time_of_start})
        context['result_ids'] = []
        context['log_id'] = []
        context['import_log'] = []
        result_pool = self.pool.get('ea_import.chain.result')
        log_pool = self.pool.get('ea_import.log')
        for chain in self.browse(cr, uid, ids, context=context):
            if not chain.type:
                raise osv.except_osv(('Error !'), ("No connection type specified!"))
            if chain.type == 'csv':
                csv_reader = unicode_csv_reader(StringIO(base64.b64decode(chain.input_file)), delimiter=str(chain.separator), quoting=(not chain.delimiter and csv.QUOTE_NONE) or csv.QUOTE_MINIMAL, quotechar=chain.delimiter and str(chain.delimiter) or None, charset=chain.charset)
            elif chain.type == 'mysql':
                # TODO add DROP|UPDATE check
                input_data = self.get_mysql_data(chain.mysql_config_id)
                csv_reader = unicode_csv_reader(StringIO(input_data), delimiter=str(chain.separator), quoting=(not chain.delimiter and csv.QUOTE_NONE) or csv.QUOTE_MINIMAL, quotechar=chain.delimiter and str(chain.delimiter) or None, charset=chain.charset)

            if chain.header:
                csv_reader.next()
            result = {}
            for chain_link in chain.link_ids:
                model_name = chain_link.template_id.target_model_id.model
                result.update({model_name: {'ids': set([]), 'post_import_hook': chain_link.post_import_hook}})
            for row_number, record_list in enumerate(csv_reader):
                if len(record_list) < max([template_line.sequence for template_line in chain_link.template_id.line_ids for chain_link in chain.link_ids]):
                    raise osv.except_osv(('Error !'), ("Invalid File or template definition. You have less columns in file than in template. Check the file separator or delimiter or template."))
                for chain_link in sorted(chain.link_ids, key=lambda k: k.sequence):
                    model_name = chain_link.template_id.target_model_id.model
                    result_id = chain_link.template_id.generate_record(record_list, row_number, context=context)
                    result[model_name]['ids'] = result[model_name]['ids'] | set(result_id)
            for name, imported_ids, post_import_hook in [(name, value['ids'], value['post_import_hook']) for name, value in result.iteritems()]:
                if post_import_hook and hasattr(self.pool.get(name), post_import_hook):
                    getattr(self.pool.get(name), post_import_hook)(cr, uid, list(imported_ids), context=context)
                result_ids_file = base64.b64encode(str(list(imported_ids)))
                result_ids = result_pool.create(cr, uid, {
                    'chain_id': chain.id,
                    'result_ids_file': result_ids_file,
                    'name': name,
                    'import_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                })
                context['result_ids'].append(result_ids)

            log_id = log_pool.create(cr, uid, {
                'chain_id': chain.id,
                'import_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })

            result_pool.write(cr, uid, context.get('result_ids', []), {'log_id': log_id}, context=context)
            log_line_obj = self.pool.get('ea_import.log.line')
            for line in context.get('import_log', []):
                log_line_obj.create(cr, uid, {
                'log_id': log_id,
                'name': line
            })
            context['log_id'].append(log_id)
        return True

    def create_action(self, cr, uid, ids, context=None):
        vals = {}
        action_obj = self.pool.get('ir.actions.act_window')
        for chain in self.browse(cr, uid, ids, context=context):
            model_name = chain.model_id.model
            button_name = 'Import from CSV (%s)' % chain.name
            vals['ir_act_window_id'] = action_obj.create(cr, uid, {
                'name': button_name,
                'type': 'ir.actions.act_window',
                'res_model': 'import_wizard',
                'src_model': model_name,
                'view_type': 'form',
                'context': "{'import_chain_id': %d}" % chain.id,
                'view_mode': 'form,tree',
                'res_id': chain.id,
                'target': 'new',
                'auto_refresh': True,
            }, context)
            vals['ir_value_id'] = self.pool.get('ir.values').create(cr, uid, {
                 'name': button_name,
                 'model': model_name,
                 'key2': 'client_action_multi',
                 'value': "ir.actions.act_window," + str(vals['ir_act_window_id']),
                 'object': True,
             }, context)
        self.write(cr, uid, ids, {
                    'ir_act_window_id': vals.get('ir_act_window_id', False),
                    'ir_value_id': vals.get('ir_value_id', False),
                }, context)
        return True

    def unlink_action(self, cr, uid, ids, context=None):
        for chain in self.browse(cr, uid, ids, context=context):
            if chain.ir_act_window_id:
                self.pool.get('ir.actions.act_window').unlink(cr, uid, chain.ir_act_window_id.id, context)
            if chain.ir_value_id:
                ir_values_obj = self.pool.get('ir.values')
                ir_values_obj.unlink(cr, uid, chain.ir_value_id.id, context)
        return True

ea_import_chain()

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
