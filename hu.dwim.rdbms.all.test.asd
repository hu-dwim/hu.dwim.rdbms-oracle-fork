;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2009 by the authors.
;;;
;;; See LICENCE for details.

(load-system :hu.dwim.asdf)

(in-package :hu.dwim.asdf)

(defsystem :hu.dwim.rdbms.all.test
  :class hu.dwim.test-system
  :package-name :hu.dwim.rdbms.test
  :depends-on (:hu.dwim.rdbms.oracle.test
               :hu.dwim.rdbms.postgresql.test
               :hu.dwim.rdbms.sqlite.test))
