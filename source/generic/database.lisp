;;; -*- mode: Lisp; Syntax: Common-Lisp; -*-
;;;
;;; Copyright (c) 2006 by the authors.
;;;
;;; See LICENCE for details.

(in-package :hu.dwim.rdbms)

;;;;;;
;;; Database

(def (special-variable e :documentation "The current database, unbound by default. Can be rebound to access multiple databases within nested dynamic scope.") *database*)

(def (class* e) database ()
  ((connection-specification
    :documentation "Backend specific connection data, usually a plist of args passed to the connect function.")
   (default-result-type
    'vector
    :type (member vector list cursor))
   (transaction-class
    :type standard-class
    :documentation "Transactions will be instances of this class. This class is created according to the generic method transaction-mixin-class.")
   (encoding
    :utf-8
    :type (member :utf-8 :us-ascii))
   (ddl-query-cache
    nil
    :type (or hash-table null))))

(def (constant e) +database-command-line-options+
  '((("database-host" #\Space)
     :type string
     :initial-value "localhost"
     :documentation "The server host name where the database is listening.")
    (("database-port" #\Space)
     :type integer
     :documentation "The server port where the database is listening.")
    (("database-name" #\Space)
     :type string
     :documentation "The database name that will be connected.")
    (("database-user-name" #\Space)
     :type string
     :documentation "The user name that is used to connect to the database.")
    (("database-password" #\Space)
     :type string
     :documentation "The password that is used to connect to the database.")))

(def (condition* e) rdbms-error ()
  ())

(def condition* translated-rdbms-error (rdbms-error)
  ((original-error)))

(def condition* simple-rdbms-error (simple-error)
  ((error-code :initarg :error-code 
			:initform nil :reader simple-rdbms-error-code)
   (message :initarg :error-message :accessor simple-rdbms-error-message)) 
  (:report (lambda (err stream)
	     (format stream "simple rdbms error: ~@[[~A]~]: ~@[~A~]"
		     (simple-rdbms-error-code err)
		     (simple-rdbms-error-message err)))))

(def function simple-rdbms-error (message &optional error-code &rest args)
  (error 'simple-rdbms-error :error-code error-code :error-message message))

(def (condition* e) unable-to-obtain-lock-error (translated-rdbms-error simple-rdbms-error)
  ())

(def function %signal-translated-simple-rdbms-error (type message-or-nested-condition)
  (error type
         :format-control (princ-to-string message-or-nested-condition)
         :original-error (when (typep message-or-nested-condition 'condition)
                           message-or-nested-condition)))

(def function unable-to-obtain-lock-error (message-or-nested-condition)
  (%signal-translated-simple-rdbms-error 'unable-to-obtain-lock-error message-or-nested-condition))

(def (condition* e) deadlock-detected-error (translated-rdbms-error simple-rdbms-error)
  ())

(def function deadlock-detected-error (message-or-nested-condition)
  (%signal-translated-simple-rdbms-error 'deadlock-detected-error message-or-nested-condition))

(def method shared-initialize :after ((database database) slot-names
                                     &key transaction-mixin generated-transaction-class-name &allow-other-keys)
  (let ((classes (mapcar #'find-class (transaction-mixin-class database))))
    (setf (transaction-class-of database)
          (make-instance 'standard-class
                         :name generated-transaction-class-name
                         :direct-superclasses (aif transaction-mixin
                                                   (cons (find-class it) classes)
                                                   classes)))))

(def (generic e) transaction-mixin-class (database)
  (:documentation "Collects the transaction mixin classes which will be inherited by the transaction class instantiated by with-transaction when using this database.")

  (:method-combination list))

(def (macro e) with-database (database &body forms)
  "Evaluates FORMS within the dynamic scope of DATABASE."
  `(let ((*database* ,database))
    ,@forms))

;;;;;;
;;; RDBMS names

(def generic calculate-rdbms-name (database thing name)
  (:documentation "May be specialized to take name length and character set limitations into account.")
  (:method ((database database) thing name)
           (string-downcase name)))

(def function rdbms-name-for (name &optional thing)
  (declare (cl:type (or null (member :table :view :index :column :sequence)) thing))
  (calculate-rdbms-name *database* thing name))

(def function calculate-rdbms-name-with-utf-8-length-limit (name limit &key prefix)
  "Cuts off the end of names that are too long and appends the hash of the original name."
  (assert (>= limit 8))
  (let ((name-as-string (string+ prefix (string-downcase name))))
    (iter (for char :in-sequence "+*\\/-~%")
          (nsubstitute #\_ char name-as-string :test #'char=))
    (let ((name-as-bytes (string-to-octets name-as-string :encoding :utf-8)))
      (when (> (length name-as-bytes)
               limit)
        (let ((hash
               (ironclad:byte-array-to-hex-string
                (ironclad:digest-sequence :crc32 name-as-bytes))))
          (iter (while (> (length name-as-bytes)
                          (- limit 8)))
                (setf name-as-string (subseq name-as-string 0 (1- (length name-as-string))))
                (setf name-as-bytes (string-to-octets name-as-string :encoding :utf-8)))
          (setf name-as-string
                (string+ name-as-string (format nil "~8,'0X" hash)))))
      name-as-string)))

(def (function e) enable-ddl-query-cache (database)
  (setf (ddl-query-cache-of database) (make-hash-table)))

(def (function e) disable-ddl-query-cache (database)
  (setf (ddl-query-cache-of database) nil))

(def (function e) clear-ddl-query-cache (database)
  (disable-ddl-query-cache database)
  (enable-ddl-query-cache database))
