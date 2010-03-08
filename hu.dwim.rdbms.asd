;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(load-system :hu.dwim.asdf)

(in-package :hu.dwim.asdf)

(defsystem :hu.dwim.rdbms
  :class hu.dwim.system
  :description "Relational database independent RDBMS and SQL abstractions."
  :depends-on (:babel
               :hu.dwim.defclass-star+hu.dwim.def
               :hu.dwim.logger
               :hu.dwim.syntax-sugar+hu.dwim.walker
               :hu.dwim.util
               :hu.dwim.walker
               :ironclad
               :local-time)
  :components ((:module "source"
                :components ((:file "package")
                             (:file "logger" :depends-on ("package"))
                             (:module "generic"
                              :depends-on ("logger")
                              :components ((:file "cursor" :depends-on ("transaction"))
                                           (:file "database")
                                           (:file "ddl" :depends-on ("transaction"))
                                           (:file "dml" :depends-on ("transaction"))
                                           (:file "transaction" :depends-on ("database"))))
                             (:module "syntax"
                              :depends-on ("generic")
                              :components ((:file "alter-table" :depends-on ("create-table"))
                                           (:file "constraint" :depends-on ("syntax"))
                                           (:file "create-table" :depends-on ("syntax" "expression"))
                                           (:file "delete" :depends-on ("syntax"))
                                           (:file "drop-table" :depends-on ("syntax"))
                                           (:file "expression" :depends-on ("syntax"))
                                           (:file "format")
                                           (:file "index" :depends-on ("syntax"))
                                           (:file "insert" :depends-on ("syntax"))
                                           (:file "lock" :depends-on ("syntax"))
                                           (:file "reader-macro")
                                           (:file "select" :depends-on ("syntax" "expression"))
                                           (:file "sequence" :depends-on ("syntax"))
                                           (:file "sexp" :depends-on ("syntax" "expression"))
                                           (:file "syntax" :depends-on ("format"))
                                           (:file "type" :depends-on ("syntax"))
                                           (:file "update" :depends-on ("syntax"))))))))

(defmethod perform ((op test-op) (system (eql (find-system :hu.dwim.rdbms))))
  (test-system :hu.dwim.rdbms.oracle)
  (test-system :hu.dwim.rdbms.postgresql)
  (test-system :hu.dwim.rdbms.sqlite))
