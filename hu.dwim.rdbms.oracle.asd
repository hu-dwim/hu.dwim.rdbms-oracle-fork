;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(load-system :hu.dwim.asdf)

(in-package :hu.dwim.asdf)

(defsystem :hu.dwim.rdbms.oracle
  :class hu.dwim.system
  :package-name :hu.dwim.rdbms
  :description "Oracle backend for hu.dwim.rdbms."
  :depends-on (:hu.dwim.rdbms
               :verrazano-runtime)
  :components ((:module "source"
                :components ((:module "oracle"
                              :components ((:file "package")
                                           (:file "cffi" :depends-on ("package"))
                                           (:file "falloc" :depends-on ("cffi"))
                                           (:file "database" :depends-on ("falloc"))
                                           (:file "syntax" :depends-on ("database"))
                                           (:file "ddl" :depends-on ("database"))
                                           (:file "cffi-util" :depends-on ("database"))
                                           (:file "conversion" :depends-on ("cffi-util"))
                                           (:file "type" :depends-on ("database"))
                                           (:file "backend" :depends-on ("cffi-util"))))))))

(defmethod perform :after ((op develop-op) (system (eql (find-system :hu.dwim.rdbms.oracle))))
  (let ((database-variable (read-from-string "hu.dwim.rdbms::*database*")))
    (unless (boundp database-variable)
      (setf (symbol-value database-variable)
            (symbol-value (read-from-string "hu.dwim.rdbms.test::*oracle-database*"))))))
