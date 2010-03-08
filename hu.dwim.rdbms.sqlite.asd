;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(load-system :hu.dwim.asdf)

(in-package :hu.dwim.asdf)

(defsystem :hu.dwim.rdbms.sqlite
  :class hu.dwim.system
  :package-name :hu.dwim.rdbms
  :description "Sqlite backend for hu.dwim.rdbms."
  :depends-on (:hu.dwim.rdbms
               :verrazano-runtime)
  :components ((:module "source"
                :components ((:module "sqlite"
                              :components ((:file "package")
                                           (:file "cffi" :depends-on ("package"))
                                           (:file "database" :depends-on ("cffi"))
                                           (:file "syntax" :depends-on ("database"))
                                           (:file "ddl" :depends-on ("database"))
                                           (:file "type" :depends-on ("database"))))))))

(defmethod perform :after ((op develop-op) (system (eql (find-system :hu.dwim.rdbms.sqlite))))
  (let ((database-variable (read-from-string "hu.dwim.rdbms::*database*")))
    (unless (boundp database-variable)
      (setf (symbol-value database-variable)
            (symbol-value (read-from-string "hu.dwim.rdbms.test::*sqlite-database*"))))))
