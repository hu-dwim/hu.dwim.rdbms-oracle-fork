;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(load-system :hu.dwim.asdf)

(in-package :hu.dwim.asdf)

(defsystem :hu.dwim.rdbms.all
  :class hu.dwim.system
  :package-name :hu.dwim.rdbms
  :description "All backends for hu.dwim.rdbms."
  :depends-on (:hu.dwim.rdbms.oracle
               :hu.dwim.rdbms.postgresql
               :hu.dwim.rdbms.sqlite))
