;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms)

(def logger rdbms ())

(def logger sql (rdbms))

(def (function e) enable-sql-log ()
  (setf (log-level 'sql) +dribble+)
  (values))

(def (function e) disable-sql-log ()
  (setf (log-level 'sql) nil) ; inherit parent
  (values))
