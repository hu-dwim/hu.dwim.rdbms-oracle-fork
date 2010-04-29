;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2009 by the authors.
;;;
;;; See LICENCE for details.

(load-system :hu.dwim.asdf)

(in-package :hu.dwim.asdf)

(defsystem :hu.dwim.rdbms.test
  :class hu.dwim.system
  :depends-on (:hu.dwim.rdbms
               :hu.dwim.stefil+hu.dwim.def+swank)
  :components ((:module "test"
                :components ((:file "basic" :depends-on ("suite"))
                             (:file "package")
                             (:file "suite" :depends-on ("package"))
                             (:file "syntax" :depends-on ("suite"))
                             (:file "type" :depends-on ("suite"))))))
