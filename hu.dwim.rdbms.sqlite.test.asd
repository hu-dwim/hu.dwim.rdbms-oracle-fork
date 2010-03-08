;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2009 by the authors.
;;;
;;; See LICENCE for details.

(load-system :hu.dwim.asdf)

(in-package :hu.dwim.asdf)

(defsystem :hu.dwim.rdbms.sqlite.test
  :class hu.dwim.test-system
  :package-name :hu.dwim.rdbms.test
  :depends-on (:hu.dwim.rdbms.sqlite
               :hu.dwim.rdbms.test)
  :components ((:module "test"
                :components ((:file "sqlite")))))

(defmethod call-in-system-environment ((operation load-op) (system (eql (find-system :hu.dwim.rdbms.sqlite.test))) function)
  (progv
      (list (read-from-string "hu.dwim.rdbms:*database*"))
      (list (eval (read-from-string "(make-instance 'hu.dwim.rdbms.sqlite:sqlite)")))
    (call-next-method)))
