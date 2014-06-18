#!/usr/bin/env python

import datetime, os, random, string

import tornado.ioloop
import tornado.web
import convert

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

EXT = set(['.xls', '.xlsx']) # accepted extensions

import sqlite3
dbconn = sqlite3.connect('products.db')
if not os.path.exists('uploads'):
  os.makedirs('uploads')
  dbconn = convert.setup_tables()

def reset():
  # clean database
  dbconn = convert.setup_tables() # resets database
  # clean up uploads folder
  for f in os.listdir('uploads'):
    path = os.path.join('uploads', f)
    if os.path.isfile(path):
      os.unlink(path)

def buildfilelist(directory):
    uploads = []
    for f in os.listdir(directory):
      (token, ext) = os.path.splitext(f)
      datestr = str(datetime.datetime.fromtimestamp(os.stat(os.path.join(directory, f)).st_atime))
      uploads.append((token,datestr))
    return uploads

class MainHandler(tornado.web.RequestHandler):
  def get(self):
    self.render("upload_form")

class UploadHandler(tornado.web.RequestHandler):
  def get(self):
    self.render("upload_form", message=None, uploads=buildfilelist('uploads'))

  def post(self):
    ofn = '' #original file name
    ufn = '' #uploaded file name
    message = ''
    uploads = buildfilelist('uploads')

    if self.request.files.get('uploaded_file'):
      uploaded_file = self.request.files['uploaded_file'][0] #input file
      ofn = uploaded_file['filename'] 
      (root, ext) = os.path.splitext(ofn)
      if ext and ext.lower() not in EXT:
        self.render("upload_form", message='unknown file extension', uploads=uploads)
        return
      import_id = ''.join(random.choice(string.ascii_lowercase + string.digits) for x in range(10))
      ufn= import_id + ext 
      with open(os.path.join('uploads', ufn), 'w') as output_file:
        output_file.write(uploaded_file.body)
        message = message + 'file uploaded successfully'
      with open(os.path.join ('uploads', ufn), 'rb') as fh:
        convert.import_xls(fh, import_id, dbconn)
      # append the file we just uploaded
      datestr = str(datetime.datetime.fromtimestamp(os.stat(os.path.join('uploads', ufn)).st_atime))
      uploads.append((import_id, datestr))
      headers=convert.get_headers(import_id, dbconn)
      rows=convert.get_import(import_id,dbconn)

    self.render("upload_form", message=message, uploads=uploads)

class ViewHandler(tornado.web.RequestHandler):
  def get(self, token):
    headers=list(convert.get_headers(token, dbconn))
    rows=list(convert.get_import(token,dbconn))
    self.render('view_page', headers=headers, rows=rows)

class ResetHandler(tornado.web.RequestHandler):
  def get(self):
    reset()
    self.render('reset_page')

application = tornado.web.Application(
    [
      (r"/", UploadHandler),
      (r"/view/([\w]+)", ViewHandler),
      (r"/reset/", ResetHandler),
    ],
    debug = True,
    template_path='templates',
    )

if __name__ == "__main__":
  port = int(os.environ.get("PORT", 5000))
  application.listen(port)
  tornado.ioloop.IOLoop.instance().start()
