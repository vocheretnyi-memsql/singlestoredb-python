
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <Python.h>

#ifndef Py_LIMITED_API
#include <datetime.h>
#endif

#define RESULTS_TYPE_TUPLES 0
#define RESULTS_TYPE_NAMEDTUPLES 1
#define RESULTS_TYPE_DICTS 2
#define RESULTS_TYPE_ARRAY 3
#define RESULTS_TYPE_DATAFRAME 4
#define RESULTS_TYPE_ARROW 5

#define MYSQL_FLAG_NOT_NULL 1
#define MYSQL_FLAG_PRI_KEY 2
#define MYSQL_FLAG_UNIQUE_KEY 4
#define MYSQL_FLAG_MULTIPLE_KEY 8
#define MYSQL_FLAG_BLOB 16
#define MYSQL_FLAG_UNSIGNED 32
#define MYSQL_FLAG_ZEROFILL 64
#define MYSQL_FLAG_BINARY 128
#define MYSQL_FLAG_ENUM 256
#define MYSQL_FLAG_AUTO_INCREMENT 512
#define MYSQL_FLAG_TIMESTAMP 1024
#define MYSQL_FLAG_SET 2048
#define MYSQL_FLAG_PART_KEY 16384
#define MYSQL_FLAG_GROUP 32767
#define MYSQL_FLAG_UNIQUE 65536

#define MYSQL_TYPE_DECIMAL 0
#define MYSQL_TYPE_TINY 1
#define MYSQL_TYPE_SHORT 2
#define MYSQL_TYPE_LONG 3
#define MYSQL_TYPE_FLOAT 4
#define MYSQL_TYPE_DOUBLE 5
#define MYSQL_TYPE_NULL 6
#define MYSQL_TYPE_TIMESTAMP 7
#define MYSQL_TYPE_LONGLONG 8
#define MYSQL_TYPE_INT24 9
#define MYSQL_TYPE_DATE 10
#define MYSQL_TYPE_TIME 11
#define MYSQL_TYPE_DATETIME 12
#define MYSQL_TYPE_YEAR 13
#define MYSQL_TYPE_NEWDATE 14
#define MYSQL_TYPE_VARCHAR 15
#define MYSQL_TYPE_BIT 16
#define MYSQL_TYPE_JSON 245
#define MYSQL_TYPE_NEWDECIMAL 246
#define MYSQL_TYPE_ENUM 247
#define MYSQL_TYPE_SET 248
#define MYSQL_TYPE_TINY_BLOB 249
#define MYSQL_TYPE_MEDIUM_BLOB 250
#define MYSQL_TYPE_LONG_BLOB 251
#define MYSQL_TYPE_BLOB 252
#define MYSQL_TYPE_VAR_STRING 253
#define MYSQL_TYPE_STRING 254
#define MYSQL_TYPE_GEOMETRY 255

#define MYSQL_TYPE_CHAR MYSQL_TYPE_TINY
#define MYSQL_TYPE_INTERVAL MYSQL_TYPE_ENUM

#define MYSQL_COLUMN_NULL 251
#define MYSQL_COLUMN_UNSIGNED_CHAR 251
#define MYSQL_COLUMN_UNSIGNED_SHORT 252
#define MYSQL_COLUMN_UNSIGNED_INT24 253
#define MYSQL_COLUMN_UNSIGNED_INT64 254

#define MYSQL_SERVER_MORE_RESULTS_EXISTS 8

// 2**24 - 1
#define MYSQL_MAX_PACKET_LEN 16777215

#define EPOCH_TO_DAYS 719528
#define SECONDS_PER_DAY (24 * 60 * 60)

#define MYSQLSV_OPTION_TIME_TYPE_TIMEDELTA 0
#define MYSQLSV_OPTION_TIME_TYPE_TIME 1
#define MYSQLSV_OPTION_JSON_TYPE_STRING 0
#define MYSQLSV_OPTION_JSON_TYPE_OBJ 1
#define MYSQLSV_OPTION_BIT_TYPE_BYTES 0
#define MYSQLSV_OPTION_BIT_TYPE_INT 1

#define CHR2INT1(x) ((x)[1] - '0')
#define CHR2INT2(x) ((((x)[0] - '0') * 10) + ((x)[1] - '0'))
#define CHR2INT3(x) ((((x)[0] - '0') * 1e2) + (((x)[1] - '0') * 10) + ((x)[2] - '0'))
#define CHR2INT4(x) ((((x)[0] - '0') * 1e3) + (((x)[1] - '0') * 1e2) + (((x)[2] - '0') * 10) + ((x)[3] - '0'))
#define CHR2INT6(x) ((((x)[0] - '0') * 1e5) + (((x)[1] - '0') * 1e4) + (((x)[2] - '0') * 1e3) + (((x)[3] - '0') * 1e2) + (((x)[4] - '0') * 10) + (((x)[5] - '0')))

#define CHECK_DATE_STR(s, s_l) \
    ((s_l) == 10 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] == '-' && \
     (((s)[5] == '1' && ((s)[6] >= '0' && (s)[6] <= '2')) || \
      ((s)[5] == '0' && ((s)[6] >= '1' && (s)[6] <= '9'))) && \
     (s)[7] == '-' && \
     ((((s)[8] >= '0' && (s)[8] <= '2') && ((s)[9] >= '0' && (s)[9] <= '9')) || \
       ((s)[8] == '3' && ((s)[9] >= '0' && (s)[9] <= '1'))) && \
       !((s)[0] == '0' && (s)[1] == '0' && (s)[2] == '0' && (s)[3] == '0') && \
       !((s)[5] == '0' && (s)[6] == '0') && \
       !((s)[8] == '0' && (s)[9] == '0'))

#define CHECK_TIME_STR(s, s_l) \
    ((s_l) == 8 && \
     ((((s)[0] >= '0' && (s)[0] <= '1') && ((s)[1] >= '0' && (s)[1] <= '9')) || \
       ((s)[0] == '2' && ((s)[1] >= '0' && (s)[1] <= '3'))) && \
     (s)[2] == ':' && \
     (((s)[3] >= '0' && (s)[3] <= '5') && ((s)[4] >= '0' && (s)[4] <= '9')) && \
     (s)[5] == ':' && \
     (((s)[6] >= '0' && (s)[6] <= '5') && ((s)[7] >= '0' && (s)[7] <= '9')))

#define CHECK_MICROSECONDS_STR(s, s_l) \
    ((s_l) == 7 && \
     (s)[0] == '.' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] >= '0' && (s)[4] <= '9' && \
     (s)[5] >= '0' && (s)[5] <= '9' && \
     (s)[6] >= '0' && (s)[6] <= '9')

#define CHECK_MILLISECONDS_STR(s, s_l) \
    ((s_l) == 4 && \
     (s)[0] == '.' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     (s)[2] >= '0' && (s)[2] <= '9' && \
     (s)[3] >= '0' && (s)[3] <= '9')

#define CHECK_MICRO_TIME_STR(s, s_l) \
    ((s_l) == 15 && CHECK_TIME_STR(s, 8) && CHECK_MICROSECONDS_STR((s)+8, 7))

#define CHECK_MILLI_TIME_STR(s, s_l) \
    ((s_l) == 12 && CHECK_TIME_STR(s, 8) && CHECK_MILLISECONDS_STR((s)+8, 4))

#define CHECK_DATETIME_STR(s, s_l) \
    ((s_l) == 19 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_TIME_STR((s)+11, 8))

#define CHECK_MICRO_DATETIME_STR(s, s_l) \
    ((s_l) == 26 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_MICRO_TIME_STR((s)+11, 15))

#define CHECK_MILLI_DATETIME_STR(s, s_l) \
    ((s_l) == 23 && \
     CHECK_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_MICRO_TIME_STR((s)+11, 12))

#define CHECK_ANY_DATETIME_STR(s, s_l) \
    (((s_l) == 19 && CHECK_DATETIME_STR(s, s_l)) || \
     ((s_l) == 23 && CHECK_MILLI_DATETIME_STR(s, s_l)) || \
     ((s_l) == 26 && CHECK_MICRO_DATETIME_STR(s, s_l)))

#define DATETIME_SIZE (19)
#define DATETIME_MILLI_SIZE (23)
#define DATETIME_MICRO_SIZE (26)

#define IS_DATETIME_MILLI(s, s_l) ((s_l) == 23)
#define IS_DATETIME_MICRO(s, s_l) ((s_l) == 26)

#define CHECK_ANY_TIME_STR(s, s_l) \
    (((s_l) == 8 && CHECK_TIME_STR(s, s_l)) || \
     ((s_l) == 12 && CHECK_MILLI_TIME_STR(s, s_l)) || \
     ((s_l) == 15 && CHECK_MICRO_TIME_STR(s, s_l)))

#define TIME_SIZE (8)
#define TIME_MILLI_SIZE (12)
#define TIME_MICRO_SIZE (15)

#define IS_TIME_MILLI(s, s_l) ((s_l) == 12)
#define IS_TIME_MICRO(s, s_l) ((s_l) == 15)


// 0000-00-00 00:00:00
// 0000-00-00 00:00:00.000
// 0000-00-00 00:00:00.000000
#define CHECK_ANY_ZERO_DATETIME_STR(s, s_l) \
    (((s_l) == 19 && CHECK_ZERO_DATETIME_STR(s, s_l)) || \
     ((s_l) == 23 && CHECK_ZERO_MILLI_DATETIME_STR(s, s_l)) || \
     ((s_l) == 26 && CHECK_ZERO_MICRO_DATETIME_STR(s, s_l)))

#define CHECK_ZERO_DATETIME_STR(s, s_l) \
    (s_l == 19 && \
     CHECK_ZERO_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_ZERO_TIME_STR((s)+11, 8))

#define CHECK_ZERO_MILLI_DATETIME_STR(s, s_l) \
    (s_l == 23 && \
     CHECK_ZERO_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_ZERO_MILLI_TIME_STR((s)+11, 12))

#define CHECK_ZERO_MICRO_DATETIME_STR(s, s_l) \
    (s_l == 26 && \
     CHECK_ZERO_DATE_STR(s, 10) && \
     ((s)[10] == ' ' || (s)[10] == 'T') && \
     CHECK_ZERO_MICRO_TIME_STR((s)+11, 15))

#define CHECK_ZERO_DATE_STR(s, s_l) \
    (s_l == 10 && ((s)[0] == '0' && (s)[1] == '0' && (s)[2] == '0' && (s)[3] == '0' && \
     (s)[4] == '-' && (s)[5] == '0' && (s)[6] == '0' && (s)[7] == '-' && \
     (s)[8] == '0' && (s)[9] == '0'))

#define CHECK_ZERO_TIME_STR(s, s_l) \
    (s_l == 8 && ((s)[0] == '0' && (s)[1] == '0' && (s)[2] == ':' && \
     (s)[3] == '0' && (s)[4] == '0' && (s)[5] == ':' && \
     (s)[6] == '0' && (s)[7] == '0'))

#define CHECK_ZERO_MILLI_TIME_STR(s, s_l) \
    (s_l == 12 && CHECK_ZERO_TIME_STR(s, 8) && \
     (s)[8] == '.' && (s)[9] == '0' && (s)[10] == '0' && (s)[11] == '0')

#define CHECK_ZERO_MICRO_TIME_STR(s, s_l) \
    (s_l == 15 && CHECK_ZERO_TIME_STR(s, 8) && \
     (s)[8] == '.' && (s)[9] == '0' && (s)[10] == '0' && (s)[11] == '0' && \
                      (s)[12] == '0' && (s)[13] == '0' && (s)[14] == '0')


#define CHECK_TIMEDELTA1_STR(s, s_l) \
    ((s_l) == 7 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] == ':' && \
     (s)[2] >= '0' && (s)[2] <= '5' && \
     (s)[3] >= '0' && (s)[3] <= '9' && \
     (s)[4] == ':' && \
     (s)[5] >= '0' && (s)[5] <= '5' && \
     (s)[6] >= '0' && (s)[6] <= '9')

#define CHECK_TIMEDELTA1_MILLI_STR(s, s_l) \
    ((s_l) == 11 && CHECK_TIMEDELTA1_STR(s, 7) && CHECK_MILLISECONDS_STR((s)+7, 4))

#define CHECK_TIMEDELTA1_MICRO_STR(s, s_l) \
    ((s_l) == 14 && CHECK_TIMEDELTA1_STR(s, 7) && CHECK_MICROSECONDS_STR((s)+7, 7))

#define CHECK_TIMEDELTA2_STR(s, s_l) \
    ((s_l) == 8 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     CHECK_TIMEDELTA1_STR((s)+1, 7))

#define CHECK_TIMEDELTA2_MILLI_STR(s, s_l) \
    ((s_l) == 12 && CHECK_TIMEDELTA2_STR(s, 8) && CHECK_MILLISECONDS_STR((s)+8, 4))

#define CHECK_TIMEDELTA2_MICRO_STR(s, s_l) \
    ((s_l) == 15 && CHECK_TIMEDELTA2_STR(s, 8) && CHECK_MICROSECONDS_STR((s)+8, 7))

#define CHECK_TIMEDELTA3_STR(s, s_l) \
    ((s_l) == 9 && \
     (s)[0] >= '0' && (s)[0] <= '9' && \
     (s)[1] >= '0' && (s)[1] <= '9' && \
     CHECK_TIMEDELTA1_STR((s)+2, 7))

#define CHECK_TIMEDELTA3_MILLI_STR(s, s_l) \
    ((s_l) == 13 && CHECK_TIMEDELTA3_STR(s, 9) && CHECK_MILLISECONDS_STR((s)+9, 4))

#define CHECK_TIMEDELTA3_MICRO_STR(s, s_l) \
    ((s_l) == 16 && CHECK_TIMEDELTA3_STR(s, 9) && CHECK_MICROSECONDS_STR((s)+9, 7))

//
// 0:00:00 / 0:00:00.000 / 0:00:00.000000
// 00:00:00 / 00:00:00.000 / 00:00:00.000000
// 000:00:00 / 000:00:00.000 / 000:00:00.000000
//
#define CHECK_ANY_TIMEDELTA_STR(s, s_l) \
    (((s_l) > 0 && (s)[0] == '-') ? \
     (-1 * (_CHECK_ANY_TIMEDELTA_STR((s)+1, (s_l)-1))) : \
     (_CHECK_ANY_TIMEDELTA_STR((s), (s_l))))

#define _CHECK_ANY_TIMEDELTA_STR(s, s_l) \
    (CHECK_TIMEDELTA1_STR(s, s_l) || \
     CHECK_TIMEDELTA2_STR(s, s_l) || \
     CHECK_TIMEDELTA3_STR(s, s_l) || \
     CHECK_TIMEDELTA1_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA2_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA3_MILLI_STR(s, s_l) || \
     CHECK_TIMEDELTA1_MICRO_STR(s, s_l) || \
     CHECK_TIMEDELTA2_MICRO_STR(s, s_l) || \
     CHECK_TIMEDELTA3_MICRO_STR(s, s_l))

#define TIMEDELTA1_SIZE (7)
#define TIMEDELTA2_SIZE (8)
#define TIMEDELTA3_SIZE (9)
#define TIMEDELTA1_MILLI_SIZE (11)
#define TIMEDELTA2_MILLI_SIZE (12)
#define TIMEDELTA3_MILLI_SIZE (13)
#define TIMEDELTA1_MICRO_SIZE (14)
#define TIMEDELTA2_MICRO_SIZE (15)
#define TIMEDELTA3_MICRO_SIZE (16)

#define IS_TIMEDELTA1(s, s_l) ((s_l) == 7 || (s_l) == 11 || (s_l) == 14)
#define IS_TIMEDELTA2(s, s_l) ((s_l) == 8 || (s_l) == 12 || (s_l) == 15)
#define IS_TIMEDELTA3(s, s_l) ((s_l) == 9 || (s_l) == 13 || (s_l) == 16)

#define IS_TIMEDELTA_MILLI(s, s_l) ((s_l) == 11 || (s_l) == 12 || (s_l) == 13)
#define IS_TIMEDELTA_MICRO(s, s_l) ((s_l) == 14 || (s_l) == 15 || (s_l) == 16)

typedef struct {
    int results_type;
    int parse_json;
    PyObject *invalid_values;
} MySQLAccelOptions;

inline int IMAX(int a, int b) { return((a) > (b) ? a : b); }
inline int IMIN(int a, int b) { return((a) < (b) ? a : b); }

#ifdef Py_LIMITED_API

char *PyUnicode_AsUTF8(PyObject *unicode) {
    PyObject *bytes = PyUnicode_AsEncodedString(unicode, "utf-8", "strict");
    if (!bytes) return NULL;

    char *str = NULL;
    Py_ssize_t str_l = 0;
    if (PyBytes_AsStringAndSize(bytes, &str, &str_l) < 0) {
	return NULL;
    }

    char *out = calloc(str_l + 1, 1);
    memcpy(out, str, str_l);
    return out;
}

#endif

//
// Cached int values for date/time components
//
static PyObject *PyInts[62] = {0};

//
// Cached string values
//
typedef struct {
    PyObject *unbuffered_active;
    PyObject *active_idx;
    PyObject *_state;
    PyObject *affected_rows;
    PyObject *warning_count;
    PyObject *connection;
    PyObject *has_next;
    PyObject *options;
    PyObject *Decimal;
    PyObject *date;
    PyObject *timedelta;
    PyObject *time;
    PyObject *datetime;
    PyObject *loads;
    PyObject *field_count;
    PyObject *converters;
    PyObject *fields;
    PyObject *flags;
    PyObject *scale;
    PyObject *type_code;
    PyObject *name;
    PyObject *table_name;
    PyObject *_sock;
    PyObject *settimeout;
    PyObject *_rfile;
    PyObject *read;
    PyObject *x_errno;
    PyObject *_result;
    PyObject *_read_timeout;
    PyObject *_next_seq_id;
    PyObject *rows;
    PyObject *NaT;
    PyObject *table;
    PyObject *DataFrame;
    PyObject *array;
    PyObject *column_stack;
    PyObject *O;
    PyObject *u1;
    PyObject *u2;
    PyObject *u4;
    PyObject *u8;
    PyObject *i1;
    PyObject *i2;
    PyObject *i4;
    PyObject *i8;
    PyObject *f4;
    PyObject *f8;
    PyObject *datetime_ns;
    PyObject *shape;
    PyObject *typestr;
    PyObject *data;
    PyObject *copy;
    PyObject *core;
    PyObject *records;
    PyObject *fromarrays;
    PyObject *names;
} PyStrings;

static PyStrings PyStr = {0};

//
// Cached Python functions
//
typedef struct {
    PyObject *json_loads;
    PyObject *decimal_Decimal;
    PyObject *datetime_date;
    PyObject *datetime_time;
    PyObject *datetime_timedelta;
    PyObject *datetime_datetime;
    PyObject *pandas_DataFrame;
    PyObject *pyarrow_table;
    PyObject *numpy_array;
    PyObject *numpy_core_records_fromarrays;
    PyObject *numpy_column_stack;
} PyFunctions;

static PyFunctions PyFunc = {0};

//
// Cached Python modules
//
typedef struct {
    PyObject *numpy;
    PyObject *datetime;
    PyObject *decimal;
    PyObject *json;
    PyObject *pandas;
    PyObject *pyarrow;
} PyModules;

static PyModules PyMod = {0};

//
// Cached Python constants
//
//typedef struct {
//    PyObject *numpy_NaT;
//} PyConstants;

//static PyConstants PyConst = {0};

//
// State
//

static PyTypeObject *StateType = NULL;

typedef struct {
    PyObject_HEAD
    PyObject *py_conn; // Database connection
    PyObject *py_fields; // List of table fields
    PyObject *py_rows; // Output object
    PyObject *py_rfile; // Socket file I/O
    PyObject *py_read; // File I/O read method
    PyObject *py_sock; // Socket
    PyObject *py_read_timeout; // Socket read timeout value
    PyObject *py_settimeout; // Socket settimeout method
    PyObject **py_converters; // List of converter functions
    PyObject **py_names; // Column names
    PyObject *py_default_converters; // Dict of default converters
    PyTypeObject *namedtuple; // Generated namedtuple type
    PyObject **py_encodings; // Encoding for each column as Python string
    PyObject **py_invalid_values; // Values to use when invalid data exists in a cell
    const char **encodings; // Encoding for each column
    unsigned long long n_cols; // Total number of columns
    unsigned long long n_rows; // Total number of rows read
    unsigned long long n_rows_in_batch; // Number of rows in current batch (fetchmany size)
    unsigned long *type_codes; // Type code for each column
    unsigned long *flags; // Column flags
    unsigned long *scales; // Column scales
    unsigned long *offsets; // Column offsets in buffer
    unsigned long long next_seq_id; // MySQL packet sequence number
    MySQLAccelOptions options; // Packet reader options
    PyStructSequence_Desc namedtuple_desc;
    int unbuffered; // Are we running in unbuffered mode?
    int is_eof; // Have we hit the eof packet yet?
    float float_nan; // Only used in numpy output
    double double_nan; // Only used in numpy output
    char *output_file; // File prefix for parquet output
    char **buffer; // Array buffers for numpy output
    unsigned long long buffer_n_rows; // Total numbef or allocated rows in numpy output
    unsigned long long buffer_row_idx; // The next row to be filled in numpy output
} StateObject;

static void read_options(MySQLAccelOptions *options, PyObject *dict);

#define DESTROY(x) do { if (x) { free((void*)x); (x) = NULL; } } while (0)

static void State_clear_fields(StateObject *self) {
    if (!self) return;
    DESTROY(self->offsets);
    DESTROY(self->scales);
    DESTROY(self->flags);
    DESTROY(self->type_codes);
    DESTROY(self->encodings);
    DESTROY(self->namedtuple_desc.fields);
    if (self->py_converters) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_converters[i]);
        }
        DESTROY(self->py_converters);
    }
    if (self->py_names) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_names[i]);
        }
        DESTROY(self->py_names);
    }
    if (self->py_encodings) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_encodings[i]);
        }
        DESTROY(self->py_encodings);
    }
    if (self->py_invalid_values) {
        for (unsigned long i = 0; i < self->n_cols; i++) {
            Py_CLEAR(self->py_invalid_values[i]);
        }
        DESTROY(self->py_invalid_values);
    }
    //if (self->buffer) {
    //    for (unsigned long i = 0; i < self->n_cols; i++) {
    //        DESTROY(self->buffer[i]);
    //    }
    //    DESTROY(self->buffer);
    //}
    Py_CLEAR(self->namedtuple);
    Py_CLEAR(self->py_default_converters);
    Py_CLEAR(self->py_settimeout);
    Py_CLEAR(self->py_read_timeout);
    Py_CLEAR(self->py_sock);
    Py_CLEAR(self->py_read);
    Py_CLEAR(self->py_rfile);
    Py_CLEAR(self->py_rows);
    Py_CLEAR(self->py_fields);
    Py_CLEAR(self->py_conn);
}

static void State_dealloc(StateObject *self) {
    State_clear_fields(self);
    PyObject_Del(self);
}

static int State_init(StateObject *self, PyObject *args, PyObject *kwds) {
    int rc = 0;
    PyObject *py_res = NULL;
    PyObject *py_converters = NULL;
    PyObject *py_options = NULL;
    unsigned long long requested_n_rows = 0;

    if (!PyArg_ParseTuple(args, "OK", &py_res, &requested_n_rows)) {
        return -1;
    }

    py_options = PyObject_GetAttr(py_res, PyStr.options);
    if (!py_options) {
        Py_INCREF(Py_None);
        py_options = Py_None;
    }

    if (PyDict_Check(py_options)) {
        self->py_default_converters = PyDict_GetItemString(py_options, "default_converters");
        if (self->py_default_converters && !PyDict_Check(self->py_default_converters)) {
            self->py_default_converters = NULL;
        }
        Py_XINCREF(self->py_default_converters);
        PyObject *py_unbuffered = PyDict_GetItemString(py_options, "unbuffered");
        if (py_unbuffered && PyObject_IsTrue(py_unbuffered)) {
            self->unbuffered = 1;
        }
    }

    if (self->unbuffered) {
        PyObject *unbuffered_active = PyObject_GetAttr(py_res, PyStr.unbuffered_active);
        if (!unbuffered_active || !PyObject_IsTrue(unbuffered_active)) {
            Py_XDECREF(unbuffered_active);
            goto error;
        }
        Py_XDECREF(unbuffered_active);
    }

    self->float_nan = nanf("");
    self->double_nan = nan("");

    // Retrieve type codes for each column.
    PyObject *py_field_count = PyObject_GetAttr(py_res, PyStr.field_count);
    if (!py_field_count) goto error;
    self->n_cols = PyLong_AsUnsignedLong(py_field_count);
    Py_XDECREF(py_field_count);

    py_converters = PyObject_GetAttr(py_res, PyStr.converters);
    if (!py_converters) goto error;

    self->py_converters = calloc(self->n_cols, sizeof(PyObject*));
    if (!self->py_converters) goto error;

    self->type_codes = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->type_codes) goto error;

    self->flags = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->flags) goto error;

    self->scales = calloc(self->n_cols, sizeof(unsigned long));
    if (!self->scales) goto error;

    self->encodings = calloc(self->n_cols, sizeof(char*));
    if (!self->encodings) goto error;

    self->py_encodings = calloc(self->n_cols, sizeof(char*));
    if (!self->py_encodings) goto error;

    self->py_invalid_values = calloc(self->n_cols, sizeof(char*));
    if (!self->py_invalid_values) goto error;

    self->py_names = calloc(self->n_cols, sizeof(PyObject*));
    if (!self->py_names) goto error;

    self->py_fields = PyObject_GetAttr(py_res, PyStr.fields);
    if (!self->py_fields) goto error;

    for (unsigned long i = 0; i < self->n_cols; i++) {
        // Get type codes.
        PyObject *py_field = PyList_GetItem(self->py_fields, i);
        if (!py_field) goto error;

        PyObject *py_flags = PyObject_GetAttr(py_field, PyStr.flags);
        if (!py_flags) goto error;
        self->flags[i] = PyLong_AsUnsignedLong(py_flags);
        Py_XDECREF(py_flags);

        PyObject *py_scale = PyObject_GetAttr(py_field, PyStr.scale);
        if (!py_scale) goto error;
        self->scales[i] = PyLong_AsUnsignedLong(py_scale);
        Py_XDECREF(py_scale);

        PyObject *py_field_type = PyObject_GetAttr(py_field, PyStr.type_code);
        if (!py_field_type) goto error;
        self->type_codes[i] = PyLong_AsUnsignedLong(py_field_type);
        PyObject *py_default_converter = (self->py_default_converters) ?
                      PyDict_GetItem(self->py_default_converters, py_field_type) : NULL;
        PyObject *py_invalid_value = (self->options.invalid_values) ?
                      PyDict_GetItem(self->options.invalid_values, py_field_type) : NULL;
        Py_XDECREF(py_field_type);

        // Get field name.
        PyObject *py_field_name = PyObject_GetAttr(py_field, PyStr.name);
        if (!py_field_name) goto error;

        // Make sure field name is not a duplicate.
        int dup_found = 0;
        for (unsigned long j = 0; j < i; j++) {
            if (PyUnicode_Compare(self->py_names[j], py_field_name) == 0) {
                dup_found = 1;
                break;
            }
        }
        if (dup_found) {
            PyObject *py_table_name = PyObject_GetAttr(py_field, PyStr.table_name);
            self->py_names[i] = PyUnicode_FromFormat("%U.%U", py_table_name, py_field_name);
            Py_XDECREF(py_table_name);
            if (!self->py_names[i]) goto error;
        } else {
            self->py_names[i] = py_field_name;
        }

        // Get field encodings (NULL means binary) and default converters.
        PyObject *py_tmp = PyList_GetItem(py_converters, i);
        if (!py_tmp) goto error;
        PyObject *py_encoding = PyTuple_GetItem(py_tmp, 0);
        if (!py_encoding) goto error;
        PyObject *py_converter = PyTuple_GetItem(py_tmp, 1);
        if (!py_converter) goto error;

        self->py_encodings[i] = (py_encoding == Py_None) ? NULL : py_encoding;
        Py_XINCREF(self->py_encodings[i]);

        self->encodings[i] = (!py_encoding || py_encoding == Py_None) ?
                              NULL : PyUnicode_AsUTF8(py_encoding);

        self->py_invalid_values[i] = (!py_invalid_value || py_invalid_value == Py_None) ?
                                      NULL : py_converter;
        Py_XINCREF(self->py_invalid_values[i]);

        self->py_converters[i] = (!py_converter
                                  || py_converter == Py_None
                                  || py_converter == py_default_converter) ?
                                 NULL : py_converter;
        Py_XINCREF(self->py_converters[i]);
    }

    // Loop over all data packets.
    self->py_conn = PyObject_GetAttr(py_res, PyStr.connection);
    if (!self->py_conn) goto error;

    // Cache socket timeout and read methods.
    self->py_sock = PyObject_GetAttr(self->py_conn, PyStr._sock);
    if (!self->py_sock) goto error;
    self->py_settimeout = PyObject_GetAttr(self->py_sock, PyStr.settimeout);
    if (!self->py_settimeout) goto error;
    self->py_read_timeout = PyObject_GetAttr(self->py_conn, PyStr._read_timeout);
    if (!self->py_read_timeout) goto error;

    self->py_rfile = PyObject_GetAttr(self->py_conn, PyStr._rfile);
    if (!self->py_rfile) goto error;
    self->py_read = PyObject_GetAttr(self->py_rfile, PyStr.read);
    if (!self->py_read) goto error;

    PyObject *py_next_seq_id = PyObject_GetAttr(self->py_conn, PyStr._next_seq_id);
    if (!py_next_seq_id) goto error;
    self->next_seq_id = PyLong_AsUnsignedLongLong(py_next_seq_id);
    Py_XDECREF(py_next_seq_id);

    if (py_options && PyDict_Check(py_options)) {
        read_options(&self->options, py_options);
    }

    switch (self->options.results_type) {
    case RESULTS_TYPE_ARRAY:
    case RESULTS_TYPE_DATAFRAME:
    case RESULTS_TYPE_ARROW:
        break;

    case RESULTS_TYPE_NAMEDTUPLES:
        self->namedtuple_desc.name = "singlestoredb.Row";
        self->namedtuple_desc.doc = "Row of data values";
        self->namedtuple_desc.n_in_sequence = self->n_cols;
        self->namedtuple_desc.fields = calloc(self->n_cols + 1, sizeof(PyStructSequence_Field));
        if (!self->namedtuple_desc.fields) goto error;
        for (unsigned long i = 0; i < self->n_cols; i++) {
            self->namedtuple_desc.fields[i].name = PyUnicode_AsUTF8(self->py_names[i]);
            self->namedtuple_desc.fields[i].doc = NULL;
        }
        self->namedtuple = PyStructSequence_NewType(&self->namedtuple_desc);
        if (!self->namedtuple) goto error;

        // Fall through

    default:
        self->py_rows = PyList_New(0);
        if (!self->py_rows) goto error;

        PyObject_SetAttr(py_res, PyStr.rows, self->py_rows);
    }

exit:
    Py_XDECREF(py_converters);
    Py_XDECREF(py_options);
    return rc;

error:
    State_clear_fields(self);
    rc = -1;
    goto exit;
}

static int State_reset_batch(
    StateObject *self,
    PyObject *py_res,
    unsigned long long requested_n_rows
) {
    int rc = 0;
    PyObject *py_tmp = NULL;

    self->n_rows_in_batch = 0;

    switch (self->options.results_type) {
    case RESULTS_TYPE_ARRAY:
    case RESULTS_TYPE_DATAFRAME:
    case RESULTS_TYPE_ARROW:
        break;
    default:
        py_tmp = self->py_rows;
        self->py_rows = PyList_New(0);
        Py_XDECREF(py_tmp);
        if (!self->py_rows) { rc = -1; goto error; }
        rc = PyObject_SetAttr(py_res, PyStr.rows, self->py_rows);
    }

exit:
    return rc;

error:
    goto exit;
}

static PyType_Slot StateType_slots[] = {
    {Py_tp_init, (initproc)State_init},
    {Py_tp_dealloc, (destructor)State_dealloc},
    {Py_tp_doc, "PyMySQL accelerator state"},
    {0, NULL},
};

static PyType_Spec StateType_spec = {
    .name = "_singlestoredb_accel.State",
    .basicsize = sizeof(StateObject),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots = StateType_slots,
};

//
// End State
//

//
// Array
//

static PyTypeObject *ArrayType = NULL;

typedef struct {
    PyObject_HEAD
    PyObject *py_array_interface;
} ArrayObject;

static void Array_dealloc(ArrayObject *self) {
    if (self->py_array_interface) {
        PyObject *data = PyDict_GetItem(self->py_array_interface, PyStr.data);
        if (data) {
            PyObject *buffer = PyTuple_GetItem(data, 0);
            if (buffer) {
                //free((char*)PyLong_AsUnsignedLongLong(buffer));
            }
        }
    }
    PyObject_Del(self);
}

static int Array_init(ArrayObject *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {"array_interface", NULL};
    PyObject *py_array_interface = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O", kwlist, &py_array_interface)) {
        return -1;
    }

    if (py_array_interface) {
        PyObject *tmp = self->py_array_interface;
        Py_INCREF(py_array_interface);
        self->py_array_interface = py_array_interface;
        Py_XDECREF(tmp);
    }

    return 0;
}

static PyObject *Array_get__array_interface__(ArrayObject *self, void *closure) {
    Py_INCREF(self->py_array_interface);
    return self->py_array_interface;
}

static PyGetSetDef Array_getsetters[] = {
    {"__array_interface__", (getter)Array_get__array_interface__,
                            (setter)NULL, "array interface", NULL},
    {NULL}
};

static PyType_Slot ArrayType_slots[] = {
    {Py_tp_init, (initproc)Array_init},
    {Py_tp_dealloc, (destructor)Array_dealloc},
    {Py_tp_doc, "Array interface"},
    {Py_tp_getset, Array_getsetters},
    {0, NULL},
};

static PyType_Spec ArrayType_spec = {
    .name = "_singlestoredb_accel.Array",
    .basicsize = sizeof(ArrayObject),
    .itemsize = 0,
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .slots = ArrayType_slots,
};

//
// End Array
//

static void read_options(MySQLAccelOptions *options, PyObject *dict) {
    if (!options || !dict) return;

    PyObject *key = NULL;
    PyObject *value = NULL;
    Py_ssize_t pos = 0;

    while (PyDict_Next(dict, &pos, &key, &value)) {
        if (PyUnicode_CompareWithASCIIString(key, "results_type") == 0) {
            if (PyUnicode_CompareWithASCIIString(value, "dict") == 0 ||
                PyUnicode_CompareWithASCIIString(value, "dicts") == 0 ) {
                options->results_type = RESULTS_TYPE_DICTS;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "namedtuple") == 0 ||
                     PyUnicode_CompareWithASCIIString(value, "namedtuples") == 0) {
                options->results_type = RESULTS_TYPE_NAMEDTUPLES;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "array") == 0) {
                options->results_type = RESULTS_TYPE_ARRAY;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "dataframe") == 0) {
                options->results_type = RESULTS_TYPE_DATAFRAME;
            }
            else if (PyUnicode_CompareWithASCIIString(value, "arrow") == 0) {
                options->results_type = RESULTS_TYPE_ARROW;
            }
            else {
                options->results_type = RESULTS_TYPE_TUPLES;
            }
        } else if (PyUnicode_CompareWithASCIIString(key, "parse_json") == 0) {
            options->parse_json = PyObject_IsTrue(value);
        } else if (PyUnicode_CompareWithASCIIString(key, "invalid_values") == 0) {
            if (PyDict_Check(value)) {
                options->invalid_values = value;
            }
        }
    }
}

// mysql, for whatever reason, treats 0 as an actual year, but not
// a leap year
//
static int is_leap_year(int year)
{
    return (year % 4) == 0 && year != 0 && ((year % 100) != 0 || (year % 400) == 0);
}

static int days_in_previous_months(int month, int year)
{
    static const int previous_days[13] =
        {
            -31,
            0,
            31,
            31 + 28,
            31 + 28 + 31,
            31 + 28 + 31 + 30,
            31 + 28 + 31 + 30 + 31,
            31 + 28 + 31 + 30 + 31 + 30,
            31 + 28 + 31 + 30 + 31 + 30 + 31,
            31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
            31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
            31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
            31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
        };
    return previous_days[month] + (month > 2 && is_leap_year(year));
}

// NOTE: year 0 does not actually exist, but mysql pretends it does (and is NOT
// a leap year)
//
static int leap_years_before(int year)
{
    return (year - 1) / 4 - (year - 1) / 100 + (year - 1) / 400;
}

static int days_in_previous_years(int year)
{
    return 365 * year + leap_years_before(year);
}

static int64_t to_days(int year, int month, int day) {
    return days_in_previous_years(year) + days_in_previous_months(month, year) + day;
}

static void raise_exception(
    PyObject *self,
    char *err_type,
    unsigned long long err_code,
    char *err_str
) {
    PyObject *py_exc = NULL;
    PyObject *py_val = NULL;

    py_exc = PyObject_GetAttrString(self, err_type);
    if (!py_exc) goto error;

    py_val = Py_BuildValue("(Ks)", err_code, err_str);
    if (!py_val) goto error;

    PyErr_SetObject(py_exc, py_val);

exit:
    if (py_exc) { Py_DECREF(py_exc); }
    if (py_val) { Py_DECREF(py_val); }
    return;

error:
    goto exit;
}

static int is_error_packet(char *buff_bytes) {
    return buff_bytes && *(uint8_t*)buff_bytes == 0xFF;
}

static void force_close(PyObject *py_conn) {
    PyObject *py_sock = NULL;

    py_sock = PyObject_GetAttr(py_conn, PyStr._sock);
    if (!py_sock) goto error;

    Py_XDECREF(PyObject_CallMethod(py_sock, "close", NULL));
    PyErr_Clear();

    PyObject_SetAttr(py_conn, PyStr._sock, Py_None);
    PyObject_SetAttr(py_conn, PyStr._rfile, Py_None);

exit:
    Py_XDECREF(py_sock);
    return;

error:
    goto exit;
}

static PyObject *read_bytes(StateObject *py_state, unsigned long long num_bytes) {
    PyObject *py_num_bytes = NULL;
    PyObject *py_data = NULL;
    PyObject *py_exc = NULL;

    if (py_state->py_read_timeout && py_state->py_read_timeout != Py_None) {
        Py_XDECREF(PyObject_CallFunctionObjArgs(py_state->py_settimeout,
                                                py_state->py_read_timeout, NULL));
        if (PyErr_Occurred()) goto error;
    }

    py_num_bytes = PyLong_FromUnsignedLongLong(num_bytes);
    if (!py_num_bytes) goto error;

    while (1) {
        py_data = PyObject_CallFunctionObjArgs(py_state->py_read, py_num_bytes, NULL);

        if ((py_exc = PyErr_Occurred())) {
            if (PyErr_ExceptionMatches(PyExc_IOError) || PyErr_ExceptionMatches(PyExc_OSError)) {
                PyObject *py_errno = PyObject_GetAttr(py_exc, PyStr.x_errno);
                if (!py_errno) goto error;

                unsigned long long err = PyLong_AsUnsignedLongLong(py_errno);
                Py_DECREF(py_errno);

                if (err == 4 /* errno.EINTER */) {
                    continue;
                }

                force_close(py_state->py_conn);
                raise_exception(py_state->py_conn, "OperationalError", 0,
                                "Lost connection to SingleStoreDB server during query");
                goto error;
            }
            else if (PyErr_ExceptionMatches(PyExc_BaseException)) {
                // Don't convert unknown exception to MySQLError.
                force_close(py_state->py_conn);
                goto error;
            }
        }

        if (py_data) {
            break;
        }
    }

    if (PyBytes_Size(py_data) < (long int)num_bytes) {
        force_close(py_state->py_conn);
        raise_exception(py_state->py_conn, "OperationalError", 0,
                        "Lost connection to SingleStoreDB server during query");
        goto error;
    }

exit:
    Py_XDECREF(py_num_bytes);
    return py_data;

error:
    Py_CLEAR(py_data);
    goto exit;
}

static PyObject *read_packet(StateObject *py_state) {
    PyObject *py_buff = NULL;
    PyObject *py_new_buff = NULL;
    PyObject *py_packet_header = NULL;
    PyObject *py_bytes_to_read = NULL;
    PyObject *py_recv_data = NULL;
    unsigned long long bytes_to_read = 0;
    char *buff = NULL;
    uint64_t btrl = 0;
    uint8_t btrh = 0;
    uint8_t packet_number = 0;

    py_buff = PyByteArray_FromStringAndSize(NULL, 0);
    if (!py_buff) goto error;

    while (1) {
        py_packet_header = read_bytes(py_state, 4);
        if (!py_packet_header) goto error;

        buff = PyBytes_AsString(py_packet_header);

        btrl = *(uint16_t*)buff;
        btrh = *(uint8_t*)(buff+2);
        packet_number = *(uint8_t*)(buff+3);
        bytes_to_read = btrl + (btrh << 16);

        Py_CLEAR(py_packet_header);

        if (packet_number != py_state->next_seq_id) {
            force_close(py_state->py_conn);
            if (packet_number == 0) {
                raise_exception(py_state->py_conn, "OperationalError", 0,
                                "Lost connection to SingleStoreDB server during query");

                goto error;
            }
            raise_exception(py_state->py_conn, "InternalError", 0,
                            "Packet sequence number wrong");
            goto error;
        }

        py_state->next_seq_id = (py_state->next_seq_id + 1) % 256;

        py_recv_data = read_bytes(py_state, bytes_to_read);
        if (!py_recv_data) goto error;

        py_new_buff = PyByteArray_Concat(py_buff, py_recv_data);
        Py_CLEAR(py_recv_data);
        Py_CLEAR(py_buff);
        if (!py_new_buff) goto error;

        py_buff = py_new_buff;
        py_new_buff = NULL;

        if (bytes_to_read == 0xFFFFFF) {
            continue;
        }

        if (bytes_to_read < MYSQL_MAX_PACKET_LEN) {
            break;
        }
    }

    if (is_error_packet(PyByteArray_AsString(py_buff))) {
        PyObject *py_result = PyObject_GetAttr(py_state->py_conn, PyStr._result);
        if (py_result && py_result != Py_None) {
            PyObject *py_unbuffered_active = PyObject_GetAttr(py_result, PyStr.unbuffered_active);
            if (py_unbuffered_active == Py_True) {
                PyObject_SetAttr(py_result, PyStr.unbuffered_active, Py_False);
            }
            Py_XDECREF(py_unbuffered_active);
        }
        Py_XDECREF(py_result);
        Py_XDECREF(PyObject_CallMethod(py_state->py_conn, "_raise_mysql_exception",
                                       "O", py_buff, NULL));
    }

exit:
    Py_XDECREF(py_new_buff);
    Py_XDECREF(py_bytes_to_read);
    Py_XDECREF(py_recv_data);
    Py_XDECREF(py_packet_header);
    return py_buff;

error:
    Py_CLEAR(py_buff);
    goto exit;
}

static int is_eof_packet(char *data, unsigned long long data_l) {
    return data && (uint8_t)*(uint8_t*)data == 0xFE && data_l < 9;
}

static int check_packet_is_eof(
    char **data,
    unsigned long long *data_l,
    unsigned long long *warning_count,
    int *has_next
) {
    uint16_t server_status = 0;
    if (!data || !data_l) {
        if (has_next) *has_next = 0;
        if (warning_count) *warning_count = 0;
        return 0;
    }
    if (!is_eof_packet(*data, *data_l)) {
        return 0;
    }
    *data += 1; *data_l -= 1;
    if (warning_count) *warning_count = **(uint16_t**)data;
    *data += 2; *data_l -= 2;
    server_status = **(uint16_t**)data;
    *data += 2; *data_l -= 2;
    if (has_next) *has_next = server_status & MYSQL_SERVER_MORE_RESULTS_EXISTS;
    return 1;
}

static unsigned long long read_length_encoded_integer(
    char **data,
    unsigned long long *data_l,
    int *is_null
) {
    if (is_null) *is_null = 0;

    if (!data || !data_l || *data_l == 0) {
        if (is_null) *is_null = 1;
        return 0;
    }

    uint8_t c = **(uint8_t**)data;
    *data += 1; *data_l -= 1;

    if (c == MYSQL_COLUMN_NULL) {
        if (is_null) *is_null = 1;
        return 0;
    }

    if (c < MYSQL_COLUMN_UNSIGNED_CHAR) {
        return c;
    }

    if (c == MYSQL_COLUMN_UNSIGNED_SHORT) {
        if (*data_l < 2) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint16_t out = **(uint16_t**)data;
        *data += 2; *data_l -= 2;
        return out;
    }

    if (c == MYSQL_COLUMN_UNSIGNED_INT24) {
        if (*data_l < 3) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint16_t low = **(uint8_t**)data;
        *data += 1; *data_l -= 1;
        uint16_t high = **(uint16_t**)data;
        *data += 2; *data_l -= 2;
        return low + (high << 16);
    }

    if (c == MYSQL_COLUMN_UNSIGNED_INT64) {
        if (*data_l < 8) {
            if (is_null) *is_null = 1;
            return 0;
        }
        uint64_t out = **(uint64_t**)data;
        *data += 8; *data_l -= 8;
        return out;
    }

    if (is_null) *is_null = 1;
    return 0;
}

static void read_length_coded_string(
    char **data,
    unsigned long long *data_l,
    char **out,
    unsigned long long *out_l,
    int *is_null
) {
    if (is_null) *is_null = 0;

    if (!data || !data_l || !out || !out_l) {
        if (is_null) *is_null = 1;
        return;
    }

    unsigned long long length = read_length_encoded_integer(data, data_l, is_null);

    if (is_null && *is_null) {
        return;
    }

    length = (length > *data_l) ? *data_l : length;

    *out = *data;
    *out_l = length;

    *data += length;
    *data_l -= length;

    return;
}

#ifdef Py_LIMITED_API

static PyObject *PyDate_FromDate(
    StateObject *py_state,
    int year,
    int month,
    int day
) {
    PyObject *out = NULL;
    PyObject *py_year = NULL;
    int free_year = 0;

    if (year >= 0 && year <= 60) {
       py_year = PyInts[year];
    } else {
       free_year = 1;
       py_year = PyLong_FromLong(year);
    }

    out = PyObject_CallFunctionObjArgs(
        PyFunc.datetime_date, py_year, PyInts[month], PyInts[day], NULL
    );

    if (free_year) Py_XDECREF(py_year);

    return out;
}

static PyObject *PyDelta_FromDSU(
    StateObject *py_state,
    int days,
    int seconds,
    int microseconds
) {
    PyObject *out = NULL;
    PyObject *py_days = NULL;
    PyObject *py_seconds = NULL;
    PyObject *py_microseconds = NULL;
    int free_days = 0;
    int free_seconds = 0;
    int free_microseconds = 0;

    if (days >= 0 && days <= 60) {
        py_days = PyInts[days];
    } else {
        free_days = 1;
        py_days = PyLong_FromLong(days);
    }

    if (seconds >= 0 && seconds <= 60) {
        py_seconds = PyInts[seconds];
    } else {
        free_seconds = 1;
        py_seconds = PyLong_FromLong(seconds);
    }

    if (microseconds >= 0 && microseconds <= 60) {
        py_microseconds = PyInts[microseconds];
    } else {
        free_microseconds = 1;
        py_microseconds = PyLong_FromLong(microseconds);
    }

    out = PyObject_CallFunctionObjArgs(
        PyFunc.datetime_timedelta, py_days, py_seconds, py_microseconds, NULL
    );

    if (free_days) Py_XDECREF(py_days);
    if (free_seconds) Py_XDECREF(py_seconds);
    if (free_microseconds) Py_XDECREF(py_microseconds);

    return out;
}

static PyObject *PyDateTime_FromDateAndTime(
    StateObject *py_state,
    int year,
    int month,
    int day,
    int hour,
    int minute,
    int second,
    int microsecond
) {
    PyObject *out = NULL;
    PyObject *py_year = NULL;
    PyObject *py_microsecond = NULL;
    int free_year = 0;
    int free_microsecond = 0;

    if (year >= 0 && year <= 60) {
       py_year = PyInts[year];
    } else {
       free_year = 1;
       py_year = PyLong_FromLong(year);
    }

    if (microsecond >= 0 && microsecond <= 60) {
       py_microsecond = PyInts[microsecond];
    } else {
       free_microsecond = 1;
       py_microsecond = PyLong_FromLong(microsecond);
    }

    out = PyObject_CallFunctionObjArgs(
        PyFunc.datetime_datetime, py_year, PyInts[month], PyInts[day],
        PyInts[hour], PyInts[minute], PyInts[second], py_microsecond, NULL
    );

    if (free_microsecond) Py_XDECREF(py_microsecond);
    if (free_year) Py_XDECREF(py_year);

    return out;
}

#endif

static PyObject *read_row_from_packet(
    StateObject *py_state,
    char *data,
    unsigned long long data_l
) {
    char *out = NULL;
    char *orig_out = NULL;
    unsigned long long out_l = 0;
    unsigned long long orig_out_l = 0;
    int is_null = 0;
    PyObject *py_result = NULL;
    PyObject *py_item = NULL;
    PyObject *py_str = NULL;
    char *end = NULL;

    int sign = 1;
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int microsecond = 0;

    switch (py_state->options.results_type) {
    case RESULTS_TYPE_NAMEDTUPLES: {
        if (!py_state->namedtuple) goto error;
        py_result = PyStructSequence_New(py_state->namedtuple);
        break;
        }
    case RESULTS_TYPE_DICTS:
        py_result = PyDict_New();
        break;
    default:
        py_result = PyTuple_New(py_state->n_cols);
    }

    for (unsigned long i = 0; i < py_state->n_cols; i++) {

        read_length_coded_string(&data, &data_l, &out, &out_l, &is_null);
        end = &out[out_l];

        orig_out = out;
        orig_out_l = out_l;

        py_item = Py_None;

        // Don't convert if it's a NULL.
        if (!is_null) {

            // If a converter was passed in, use it.
            if (py_state->py_converters[i]) {
                py_str = NULL;
                if (py_state->encodings[i] == NULL) {
                    py_str = PyBytes_FromStringAndSize(out, out_l);
                    if (!py_str) goto error;
                } else {
                    py_str = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
                    if (!py_str) goto error;
                }
                py_item = PyObject_CallFunctionObjArgs(py_state->py_converters[i], py_str, NULL);
                Py_CLEAR(py_str);
                if (!py_item) goto error;
            }

            // If no converter was passed in, do the default processing.
            else {
                switch (py_state->type_codes[i]) {
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_DECIMAL:
                    py_str = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
                    if (!py_str) goto error;

                    py_item = PyObject_CallFunctionObjArgs(PyFunc.decimal_Decimal, py_str, NULL);
                    Py_CLEAR(py_str);
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_INT24:
                    if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                        py_item = PyLong_FromUnsignedLongLong(strtoull(out, &end, 10));
                    } else {
                        py_item = PyLong_FromLongLong(strtoll(out, &end, 10));
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                    py_item = PyFloat_FromDouble(strtod(out, &end));
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_NULL:
                    py_item = Py_None;
                    break;

                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_TIMESTAMP:
                    if (CHECK_ANY_ZERO_DATETIME_STR(out, out_l)) {
                        py_item = Py_None;
                        Py_INCREF(Py_None);
                        break;
                    }
                    else if (!CHECK_ANY_DATETIME_STR(out, out_l)) {
                        if (py_state->py_invalid_values[i]) {
                            py_item = py_state->py_invalid_values[i];
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                            if (!py_item) goto error;
                        }
                        break;
                    }
                    year = CHR2INT4(out); out += 5;
                    month = CHR2INT2(out); out += 3;
                    day = CHR2INT2(out); out += 3;
                    hour = CHR2INT2(out); out += 3;
                    minute = CHR2INT2(out); out += 3;
                    second = CHR2INT2(out); out += 3;
                    microsecond = (IS_DATETIME_MICRO(out, out_l)) ? CHR2INT6(out) :
                                  (IS_DATETIME_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    py_item = PyDateTime_FromDateAndTime(
#ifdef Py_LIMITED_API
                                    py_state,
#endif
                                    year, month, day, hour, minute, second, microsecond);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_NEWDATE:
                case MYSQL_TYPE_DATE:
                    if (CHECK_ZERO_DATE_STR(out, out_l)) {
                        py_item = Py_None;
                        Py_INCREF(Py_None);
                        break;
                    }
                    else if (!CHECK_DATE_STR(out, out_l)) {
                        if (py_state->py_invalid_values[i]) {
                            py_item = py_state->py_invalid_values[i];
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                            if (!py_item) goto error;
                        }
                        break;
                    }
                    year = CHR2INT4(out); out += 5;
                    month = CHR2INT2(out); out += 3;
                    day = CHR2INT2(out); out += 3;
                    py_item = PyDate_FromDate(
#ifdef Py_LIMITED_API
                                    py_state,
#endif
                                    year, month, day);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_TIME:
                    sign = CHECK_ANY_TIMEDELTA_STR(out, out_l);
                    if (!sign) {
                        if (py_state->py_invalid_values[i]) {
                            py_item = py_state->py_invalid_values[i];
                            Py_INCREF(py_item);
                        } else {
                            py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                            if (!py_item) goto error;
                        }
                        break;
                    } else if (sign < 0) {
                        out += 1; out_l -= 1;
                    }
                    if (IS_TIMEDELTA1(out, out_l)) {
                        hour = CHR2INT1(out); out += 2;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    else if (IS_TIMEDELTA2(out, out_l)) {
                        hour = CHR2INT2(out); out += 3;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    else if (IS_TIMEDELTA3(out, out_l)) {
                        hour = CHR2INT3(out); out += 4;
                        minute = CHR2INT2(out); out += 3;
                        second = CHR2INT2(out); out += 3;
                        microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                                      (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
                    }
                    py_item = PyDelta_FromDSU(
#ifdef Py_LIMITED_API
                                    py_state,
#endif
                                    0, sign * hour * 60 * 60 +
                                       sign * minute * 60 +
                                       sign * second,
                                       sign * microsecond);
                    if (!py_item) {
                        PyErr_Clear();
                        py_item = PyUnicode_Decode(orig_out, orig_out_l, "utf8", "strict");
                    }
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_YEAR:
                    if (out_l == 0) {
                        goto error;
                        break;
                    }
                    end = &out[out_l];
                    year = strtoul(out, &end, 10);
                    py_item = PyLong_FromLong(year);
                    if (!py_item) goto error;
                    break;

                case MYSQL_TYPE_BIT:
                case MYSQL_TYPE_JSON:
                case MYSQL_TYPE_TINY_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_GEOMETRY:
                case MYSQL_TYPE_ENUM:
                case MYSQL_TYPE_SET:
                case MYSQL_TYPE_VARCHAR:
                case MYSQL_TYPE_VAR_STRING:
                case MYSQL_TYPE_STRING:
                    if (!py_state->encodings[i]) {
                        py_item = PyBytes_FromStringAndSize(out, out_l);
                        if (!py_item) goto error;
                        break;
                    }

                    py_item = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
                    if (!py_item) goto error;

                    // Parse JSON string.
                    if (py_state->type_codes[i] == MYSQL_TYPE_JSON && py_state->options.parse_json) {
                        py_str = py_item;
                        py_item = PyObject_CallFunctionObjArgs(PyFunc.json_loads, py_str, NULL);
                        Py_CLEAR(py_str);
                        if (!py_item) goto error;
                    }

                    break;

                default:
                    PyErr_Format(PyExc_TypeError, "Unknown type code: %ld",
                                 py_state->type_codes[i], NULL);
                    goto error;
                }
            }
        }

        if (py_item == Py_None) {
            Py_INCREF(Py_None);
        }

        switch (py_state->options.results_type) {
        case RESULTS_TYPE_NAMEDTUPLES:
            PyStructSequence_SetItem(py_result, i, py_item);
            break;
        case RESULTS_TYPE_DICTS:
            PyDict_SetItem(py_result, py_state->py_names[i], py_item);
            Py_INCREF(py_state->py_names[i]);
            Py_DECREF(py_item);
            break;
        default:
            PyTuple_SetItem(py_result, i, py_item);
        }
    }

exit:
    return py_result;

error:
    Py_CLEAR(py_result);
    goto exit;
}

static int update_arrays(
    StateObject *py_state
) {
    int rc = 0;
    int size = 0;

    // If the buffer hasn't been initialized, do it
    if (!py_state->buffer) {
        if (!py_state->buffer_n_rows) {
            py_state->buffer_n_rows = 100;
        }
        py_state->buffer = calloc(py_state->n_cols, sizeof(char*));
        // TODO: Raise Python exception
        if (!py_state->buffer) goto error;
    }

    // We haven't hit the end yet
    else if (py_state->buffer_row_idx < py_state->buffer_n_rows) {
        goto exit;
    }

    // If file output is set, purge the current rows and reset
    // the buffer back to the start
    else if (py_state->output_file) {
        // TODO: Write data to file
        py_state->buffer_row_idx = 0;
    }

    // We hit the end, extend the buffer
    else {
        py_state->buffer_n_rows *= 1.7;
    }

    for (unsigned long i = 0; i < py_state->n_cols; i++) {
        switch (py_state->type_codes[i]) {
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL:
            size = sizeof(PyObject*);
            break;
        case MYSQL_TYPE_TINY:
            size = sizeof(int8_t);
            break;
        case MYSQL_TYPE_SHORT:
            size = sizeof(int16_t);
            break;
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
            size = sizeof(int32_t);
            break;
        case MYSQL_TYPE_LONGLONG:
            size = sizeof(int64_t);
            break;
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_DOUBLE:
            size = sizeof(double);
            break;
        case MYSQL_TYPE_NULL:
            size = sizeof(PyObject*);
            break;
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            size = sizeof(int64_t);
            break;
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATE:
            size = sizeof(int64_t);
            break;
        case MYSQL_TYPE_TIME:
            size = sizeof(int64_t);
            break;
        case MYSQL_TYPE_YEAR:
            size = sizeof(uint16_t);
            break;
        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            size = sizeof(PyObject*);
            break;
        default:
            PyErr_Format(PyExc_TypeError, "Unknown type code: %ld",
                         py_state->type_codes[i], NULL);
            goto error;
        }

        if (!py_state->buffer[i]) {
            py_state->buffer[i] = calloc(py_state->buffer_n_rows, size);
        } else {
            py_state->buffer[i] = realloc(py_state->buffer[i], py_state->buffer_n_rows * size);
        }

        // TODO: Raise Python exception
        if (!py_state->buffer[i]) {
            goto error;
        }
    }

exit:
    return rc;

error:
    rc = -1;
    goto exit;
}

static PyObject *build_column(
    StateObject *py_state,
    long long int col
) {
    int rc = 0;
    PyObject *py_out = NULL;
    PyObject *py_dict = NULL;
    PyObject *py_shape = NULL;
    PyObject *py_typestr = NULL;
    PyObject *py_data = NULL;
    PyObject *py_array_args = NULL;
    PyObject *py_args = NULL;
    PyObject *py_kwds = NULL;
    ArrayObject *py_array = NULL;

    py_dict = PyDict_New();
    if (!py_dict) goto error;

    py_shape = PyTuple_New(1);
    if (!py_shape) goto error;
    rc = PyTuple_SetItem(py_shape, 0, PyLong_FromUnsignedLongLong(py_state->n_rows));
    if (rc) goto error;
    rc = PyDict_SetItem(py_dict, PyStr.shape, py_shape);
    if (rc) goto error;
    Py_DECREF(py_shape);

    switch (py_state->type_codes[col]) {
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_DECIMAL:
        py_typestr = PyStr.O;
        break;

    case MYSQL_TYPE_TINY:
        py_typestr = (py_state->flags[col] & MYSQL_FLAG_UNSIGNED) ? PyStr.u1 : PyStr.i1;
        break;

    case MYSQL_TYPE_SHORT:
        py_typestr = (py_state->flags[col] & MYSQL_FLAG_UNSIGNED) ? PyStr.u2 : PyStr.i2;
        break;

    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
        py_typestr = (py_state->flags[col] & MYSQL_FLAG_UNSIGNED) ? PyStr.u4 : PyStr.i4;
        break;

    case MYSQL_TYPE_LONGLONG:
        py_typestr = (py_state->flags[col] & MYSQL_FLAG_UNSIGNED) ? PyStr.u8 : PyStr.i8;
        break;

    case MYSQL_TYPE_FLOAT:
        py_typestr = PyStr.f4;
        break;

    case MYSQL_TYPE_DOUBLE:
        py_typestr = PyStr.f8;
        break;

    case MYSQL_TYPE_NULL:
        py_typestr = PyStr.O;
        break;

    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP:
        py_typestr = PyStr.datetime_ns;
        break;

    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATE:
        py_typestr = PyStr.datetime_ns;
        break;

    case MYSQL_TYPE_TIME:
        py_typestr = PyStr.datetime_ns;
        break;

    case MYSQL_TYPE_YEAR:
        py_typestr = PyStr.u2;
        break;

    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_JSON:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
        py_typestr = PyStr.O;
        break;

    default:
        PyErr_Format(PyExc_TypeError, "Unknown type code: %ld", py_state->type_codes[col], NULL);
        goto error;
    }

    if (!py_typestr) goto error;

    rc = PyDict_SetItem(py_dict, PyStr.typestr, py_typestr);
    if (rc) goto error;

    py_data = PyTuple_New(2);
    if (!py_data) goto error;

    rc = PyTuple_SetItem(py_data, 0, PyLong_FromUnsignedLongLong((unsigned long long)py_state->buffer[col]));
    if (rc) goto error;
    rc = PyTuple_SetItem(py_data, 1, Py_False);
    if (rc) goto error;
    Py_INCREF(Py_False);
    rc = PyDict_SetItem(py_dict, PyStr.data, py_data);
    if (rc) goto error;

    py_array_args = PyTuple_New(1);
    if (!py_array_args) goto error;
    rc = PyTuple_SetItem(py_array_args, 0, py_dict);
    if (rc) goto error;
    py_array = (ArrayObject*)PyObject_Call((PyObject*)ArrayType, py_array_args, NULL);
    if (!py_array) goto error;

    py_args = PyTuple_New(1);
    if (!py_args) goto error;
    rc = PyTuple_SetItem(py_args, 0, (PyObject*)py_array);
    if (rc) goto error;
    py_kwds = PyDict_New();
    rc = PyDict_SetItem(py_kwds, PyStr.copy, Py_False);
    if (rc) goto error;
    py_out = PyObject_Call(PyFunc.numpy_array, py_args, py_kwds);
    if (!py_out) goto error;

exit:
    Py_XDECREF(py_data);
    Py_XDECREF(py_dict);
    Py_XDECREF(py_array_args);
    Py_XDECREF(py_args);
    Py_XDECREF(py_kwds);
    return py_out;

error:
    Py_XDECREF(py_out);
    py_out = NULL;
    goto exit;
}

static PyObject *build_array(
    StateObject *py_state
) {
    int rc = 0;
    PyObject *py_out = NULL;
    PyObject *py_dict = NULL;
    PyObject *py_args = NULL;
    PyObject *py_list = NULL;
    PyObject *py_names = NULL;
    PyObject *py_kwds = NULL;

    // Numpy array
    if (py_state->options.results_type == RESULTS_TYPE_ARRAY) {
        py_args = PyTuple_New(1);
        if (!py_args) goto error;

        py_list = PyList_New(py_state->n_cols);
        if (!py_list) goto error;
        PyTuple_SetItem(py_args, 0, py_list);

        py_kwds = PyDict_New();
        if (!py_kwds) goto error;
        py_names = PyList_New(py_state->n_cols);
        if (!py_names) goto error;
        PyDict_SetItem(py_kwds, PyStr.names, py_names);

        for (unsigned long i = 0; i < py_state->n_cols; i++) {
            // TODO: Save this list in py_state for reuse
            PyList_SetItem(py_names, i, py_state->py_names[i]);
            rc = PyList_SetItem(py_list, i, build_column(py_state, i));
            if (rc != 0) goto exit;
        }

        py_out = PyObject_Call(PyFunc.numpy_core_records_fromarrays, py_args, py_kwds);
    }

    // Dataframe and Arrow Table
    else {
        py_dict = PyDict_New();
        if (!py_dict) goto error;

        for (unsigned long i = 0; i < py_state->n_cols; i++) {
            rc = PyDict_SetItem(py_dict, py_state->py_names[i], build_column(py_state, i));
            if (rc != 0) goto exit;
            Py_INCREF(py_state->py_names[i]);
        }

        py_args = PyTuple_New(1);
        if (!py_args) goto error;
        rc = PyTuple_SetItem(py_args, 0, py_dict);
        if (rc != 0) goto exit;

        switch (py_state->options.results_type) {
        case RESULTS_TYPE_DATAFRAME:
            py_out = PyObject_Call(PyFunc.pandas_DataFrame, py_args, NULL);
            break;

        case RESULTS_TYPE_ARROW:
            py_out = PyObject_Call(PyFunc.pyarrow_table, py_args, NULL);
            break;

            // TODO: If output is parquet, flush the data to disk
        }
    }

exit:
    Py_XDECREF(py_names);
    Py_XDECREF(py_kwds);
    Py_XDECREF(py_list);
    Py_XDECREF(py_dict);
    Py_XDECREF(py_args);
    return py_out;

error:
    Py_XDECREF(py_out);
    py_out = NULL;
    goto exit;
}

static int read_array_row_from_packet(
    StateObject *py_state,
    char *data,
    unsigned long long data_l
) {
    int rc = 0;
    char *out = NULL;
    unsigned long long out_l = 0;
    int is_null = 0;
    PyObject *py_item = NULL;
    PyObject *py_str = NULL;
    char *end = NULL;
    char **arrays = NULL;
    PyObject *core = NULL;
    PyObject *records = NULL;

    int sign = 1;
    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    int microsecond = 0;

    if (!PyMod.numpy) {
        PyMod.numpy = PyImport_ImportModule("numpy");
        if (!PyMod.numpy) goto error;
        //PyConst.numpy_NaT = PyObject_GetAttr(PyMod.numpy, PyStr.NaT);
        //if (!PyConst.numpy_NaT) goto error;
        core = PyObject_GetAttr(PyMod.numpy, PyStr.core);
        if (!core) goto error;
        records = PyObject_GetAttr(core, PyStr.records);
        if (!records) goto error;
        PyFunc.numpy_core_records_fromarrays = PyObject_GetAttr(records, PyStr.fromarrays);
        if (!PyFunc.numpy_core_records_fromarrays) goto error;
        PyFunc.numpy_array = PyObject_GetAttr(PyMod.numpy, PyStr.array);
        if (!PyFunc.numpy_array) goto error;
        PyFunc.numpy_column_stack = PyObject_GetAttr(PyMod.numpy, PyStr.column_stack);
        if (!PyFunc.numpy_column_stack) goto error;
    }

    switch (py_state->options.results_type) {
    case RESULTS_TYPE_DATAFRAME:
        if (!PyMod.pandas) {
            PyMod.pandas = PyImport_ImportModule("pandas");
            if (!PyMod.pandas) goto error;
            PyFunc.pandas_DataFrame = PyObject_GetAttr(PyMod.pandas, PyStr.DataFrame);
            if (!PyFunc.pandas_DataFrame) goto error;
        }
        break;
    case RESULTS_TYPE_ARROW:
        if (!PyMod.pyarrow) {
            PyMod.pyarrow = PyImport_ImportModule("pyarrow");
            if (!PyMod.pyarrow) goto error;
            PyFunc.pyarrow_table = PyObject_GetAttr(PyMod.pyarrow, PyStr.table);
            if (!PyFunc.pyarrow_table) goto error;
        }
        break;
    }

    // If we've reached the end of the array buffers, extend them.
    if ((rc = update_arrays(py_state))) {
        // TODO: Raise Python exception
        goto error;
    }

    arrays = py_state->buffer;

    for (unsigned long i = 0; i < py_state->n_cols; i++) {

        read_length_coded_string(&data, &data_l, &out, &out_l, &is_null);
        end = &out[out_l];

        switch (py_state->type_codes[i]) {
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL:
        {
            PyObject **loc = ((PyObject**)arrays[i]) + py_state->buffer_row_idx;
            if (is_null) {
                *loc = Py_None;
                Py_INCREF(Py_None);
            } else {
                py_str = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
                if (!py_str) goto error;

                py_item = PyObject_CallFunctionObjArgs(PyFunc.decimal_Decimal, py_str, NULL);
                Py_CLEAR(py_str);
                if (!py_item) goto error;

                *loc = py_item;
            }
            break;
        }

        case MYSQL_TYPE_TINY:
        {
            uint8_t *loc = ((uint8_t*)arrays[i]) + py_state->buffer_row_idx;
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                *loc = (is_null) ? 0 : (uint8_t)strtoull(out, &end, 10);
            } else {
                *loc = (is_null) ? INT8_MIN : (int8_t)strtoll(out, &end, 10);
            }
            break;
        }

        case MYSQL_TYPE_SHORT:
        {
            uint16_t *loc = ((uint16_t*)arrays[i]) + py_state->buffer_row_idx;
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                *loc = (is_null) ? 0 : (uint16_t)strtoull(out, &end, 10);
            } else {
                *loc = (is_null) ? INT16_MIN : (int16_t)strtoll(out, &end, 10);
            }
            break;
        }

        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        {
            uint32_t *loc = ((uint32_t*)arrays[i]) + py_state->buffer_row_idx;
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                *loc = (is_null) ? 0 : (uint32_t)strtoull(out, &end, 10);
            } else {
                *loc = (is_null) ? INT32_MIN : (int32_t)strtoll(out, &end, 10);
            }
            break;
        }

        case MYSQL_TYPE_LONGLONG:
        {
            uint64_t *loc = ((uint64_t*)arrays[i]) + py_state->buffer_row_idx;
            if (py_state->flags[i] & MYSQL_FLAG_UNSIGNED) {
                *loc = (is_null) ? 0 : (uint64_t)strtoull(out, &end, 10);
            } else {
                *loc = (is_null) ? INT64_MIN : (int64_t)strtoll(out, &end, 10);
            }
            break;
        }

        case MYSQL_TYPE_FLOAT:
        {
            float *loc = ((float*)arrays[i]) + py_state->buffer_row_idx;
            if (is_null) {
                *loc = (float)py_state->float_nan;
            } else {
                *loc = (float)strtod(out, &end);
            }
            break;
        }

        case MYSQL_TYPE_DOUBLE:
        {
            double *loc = ((double*)arrays[i]) + py_state->buffer_row_idx;
            if (is_null) {
                *loc = (double)py_state->double_nan;
            } else {
                *loc = (double)strtod(out, &end);
            }
            break;
        }

        case MYSQL_TYPE_NULL:
        {
            PyObject **loc = ((PyObject**)arrays[i]) + py_state->buffer_row_idx;
            *loc = Py_None;
            break;
        }

        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
        {
            int64_t *loc = ((int64_t*)arrays[i]) + py_state->buffer_row_idx;
            if (CHECK_ANY_ZERO_DATETIME_STR(out, out_l) ||
                !CHECK_ANY_DATETIME_STR(out, out_l)) {
                //*loc = (int64_t)(PyConst.numpy_NaT);
                *loc = (int64_t)(INT64_MIN);
                break;
            }
            year = CHR2INT4(out); out += 5;
            month = CHR2INT2(out); out += 3;
            day = CHR2INT2(out); out += 3;
            hour = CHR2INT2(out); out += 3;
            minute = CHR2INT2(out); out += 3;
            second = CHR2INT2(out); out += 3;
            microsecond = (IS_DATETIME_MICRO(out, out_l)) ? CHR2INT6(out) :
                          (IS_DATETIME_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
            *loc = (int64_t)(((to_days(year, month, day) - EPOCH_TO_DAYS)
                               * SECONDS_PER_DAY + hour * 3600 + minute * 60 + second)
                             * 1e9 + microsecond * 1e3);
            break;
        }

        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATE:
        {
            int64_t *loc = ((int64_t*)arrays[i]) + py_state->buffer_row_idx;
            if (CHECK_ZERO_DATE_STR(out, out_l) ||
                !CHECK_DATE_STR(out, out_l)) {
                *loc = (int64_t)(INT64_MIN);
                break;
            }
            year = CHR2INT4(out); out += 5;
            month = CHR2INT2(out); out += 3;
            day = CHR2INT2(out); out += 3;
            *loc = (int64_t)((to_days(year, month, day) - EPOCH_TO_DAYS)
                              * SECONDS_PER_DAY * 1e9);
            break;
        }

        case MYSQL_TYPE_TIME:
        {
            int64_t *loc = ((int64_t*)arrays[i]) + py_state->buffer_row_idx;
            sign = CHECK_ANY_TIMEDELTA_STR(out, out_l);
            if (!sign) {
                *loc = (int64_t)(INT64_MIN);
                break;
            } else if (sign < 0) {
                out += 1; out_l -= 1;
            }
            if (IS_TIMEDELTA1(out, out_l)) {
                hour = CHR2INT1(out); out += 2;
                minute = CHR2INT2(out); out += 3;
                second = CHR2INT2(out); out += 3;
                microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                              (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
            }
            else if (IS_TIMEDELTA2(out, out_l)) {
                hour = CHR2INT2(out); out += 3;
                minute = CHR2INT2(out); out += 3;
                second = CHR2INT2(out); out += 3;
                microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                              (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
            }
            else if (IS_TIMEDELTA3(out, out_l)) {
                hour = CHR2INT3(out); out += 4;
                minute = CHR2INT2(out); out += 3;
                second = CHR2INT2(out); out += 3;
                microsecond = (IS_TIMEDELTA_MICRO(out, out_l)) ? CHR2INT6(out) :
                              (IS_TIMEDELTA_MILLI(out, out_l)) ? CHR2INT3(out) * 1e3 : 0;
            }
            *loc = (int64_t)((hour * 3600 + minute * 60 + second)
                             * 1e9 + microsecond * 1e3) * sign;
            break;
        }

        case MYSQL_TYPE_YEAR:
        {
            uint16_t *loc = ((uint16_t*)arrays[i]) + py_state->buffer_row_idx;
            if (out_l == 0) {
                *loc = 0;
                break;
            }
            end = &out[out_l];
            *loc = (uint16_t)strtoul(out, &end, 10);
            break;
        }

        case MYSQL_TYPE_BIT:
        case MYSQL_TYPE_JSON:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_GEOMETRY:
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        {
            PyObject **loc = ((PyObject**)arrays[i]) + py_state->buffer_row_idx;

            if (!py_state->encodings[i]) {
                py_item = PyBytes_FromStringAndSize(out, out_l);
                if (!py_item) goto error;
                break;
            }

            py_item = PyUnicode_Decode(out, out_l, py_state->encodings[i], "strict");
            if (!py_item) goto error;

            // Parse JSON string.
            if (py_state->type_codes[i] == MYSQL_TYPE_JSON && py_state->options.parse_json) {
                py_str = py_item;
                py_item = PyObject_CallFunctionObjArgs(PyFunc.json_loads, py_str, NULL);
                Py_CLEAR(py_str);
                if (!py_item) goto error;
            }

            *loc = py_item;

            break;
        }

        default:
            PyErr_Format(PyExc_TypeError, "Unknown type code: %ld",
                         py_state->type_codes[i], NULL);
            goto error;
        }
    }

    py_state->buffer_row_idx++;

exit:
    Py_XDECREF(core);
    Py_XDECREF(records);
    return rc;

error:
    rc = -1;
    goto exit;
}

static PyObject *read_rowdata_packet(PyObject *self, PyObject *args, PyObject *kwargs) {
    int rc = 0;
    StateObject *py_state = NULL;
    PyObject *py_res = NULL;
    PyObject *py_unbuffered = NULL;
    PyObject *py_out = NULL;
    PyObject *py_next_seq_id = NULL;
    unsigned long long requested_n_rows = 0;
    unsigned long long row_idx = 0;
    char *keywords[] = {"result", "unbuffered", "size", NULL};

    // Parse function args.
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|K", keywords,
                                     &py_res, &py_unbuffered, &requested_n_rows)) {
        goto error;
    }

    if (py_unbuffered && PyObject_IsTrue(py_unbuffered)) {
        PyObject *unbuffered_active = PyObject_GetAttr(py_res, PyStr.unbuffered_active);
        if (!unbuffered_active || !PyObject_IsTrue(unbuffered_active)) {
            Py_XDECREF(unbuffered_active);
            Py_INCREF(Py_None);
            return Py_None;
         }
        Py_XDECREF(unbuffered_active);
    }

    // Get the rowdata state.
    py_state = (StateObject*)PyObject_GetAttr(py_res, PyStr._state);
    if (!py_state) {
        PyErr_Clear();

        PyObject *py_requested_n_rows = PyLong_FromUnsignedLongLong(requested_n_rows);
        if (!py_requested_n_rows) goto error;

        PyObject *py_args = PyTuple_New(2);
        if (!py_args) goto error;
        PyTuple_SetItem(py_args, 0, py_res);
        PyTuple_SetItem(py_args, 1, py_requested_n_rows);
        Py_INCREF(py_res);
        Py_INCREF(py_requested_n_rows);

        py_state = (StateObject*)PyObject_CallObject((PyObject*)StateType, py_args);
        if (!py_state) { Py_DECREF(py_args); goto error; }
        Py_DECREF(py_args);

        PyObject_SetAttr(py_res, PyStr._state, (PyObject*)py_state);
    }
    else if (requested_n_rows > 0) {
        State_reset_batch(py_state, py_res, requested_n_rows);
    }

    if (requested_n_rows == 0) {
        requested_n_rows = UINTMAX_MAX;
    }

    if (py_state->is_eof) {
        goto exit;
    }

    while (row_idx < requested_n_rows) {
        PyObject *py_buff = read_packet(py_state);
        if (!py_buff) goto error;

        PyObject *py_row = NULL;
        char *data = PyByteArray_AsString(py_buff);
        unsigned long long data_l = PyByteArray_Size(py_buff);
        unsigned long long warning_count = 0;
        int has_next = 0;

        if (check_packet_is_eof(&data, &data_l, &warning_count, &has_next)) {
            Py_CLEAR(py_buff);

            py_state->is_eof = 1;

            PyObject *py_long = NULL;

            py_long = PyLong_FromUnsignedLongLong(warning_count);
            PyObject_SetAttr(py_res, PyStr.warning_count, py_long ? py_long : 0);
            Py_CLEAR(py_long);

            py_long = PyLong_FromLong(has_next);
            PyObject_SetAttr(py_res, PyStr.has_next, py_long ? py_long : 0);
            Py_CLEAR(py_long);

            PyObject_SetAttr(py_res, PyStr.connection, Py_None);
            PyObject_SetAttr(py_res, PyStr.unbuffered_active, Py_False);

            break;
        }

        py_state->n_rows++;
        py_state->n_rows_in_batch++;

        switch (py_state->options.results_type) {
        case RESULTS_TYPE_DATAFRAME:
        case RESULTS_TYPE_ARRAY:
        case RESULTS_TYPE_ARROW:
            rc = read_array_row_from_packet(py_state, data, data_l);
            if (rc != 0) { Py_CLEAR(py_buff); goto error; }
            break;

        default:
            py_row = read_row_from_packet(py_state, data, data_l);
            if (!py_row) { Py_CLEAR(py_buff); goto error; }

            rc = PyList_Append(py_state->py_rows, py_row);
            Py_DECREF(py_row);
            if (rc != 0) { Py_CLEAR(py_buff); goto error; }
        }

        row_idx++;

        Py_CLEAR(py_buff);
    }

exit:
    if (!py_state) return NULL;

    py_next_seq_id = PyLong_FromUnsignedLongLong(py_state->next_seq_id);
    if (!py_next_seq_id) goto error;
    PyObject_SetAttr(py_state->py_conn, PyStr._next_seq_id, py_next_seq_id);
    Py_DECREF(py_next_seq_id);

    py_out = NULL;

    if (py_state->unbuffered) {
        if (py_state->is_eof && row_idx == 0) {
            Py_INCREF(Py_None);
            py_out = Py_None;
            PyObject_SetAttr(py_res, PyStr.rows, Py_None);
            PyObject *py_n_rows = PyLong_FromSsize_t(py_state->n_rows);
            PyObject_SetAttr(py_res, PyStr.affected_rows, (py_n_rows) ? py_n_rows : Py_None);
            Py_XDECREF(py_n_rows);
            PyObject_DelAttr(py_res, PyStr._state);
            Py_CLEAR(py_state);
        }
        else {
            switch (py_state->options.results_type) {
            case RESULTS_TYPE_ARRAY:
            case RESULTS_TYPE_DATAFRAME:
            case RESULTS_TYPE_ARROW:
                if (py_state->buffer) {
                    py_out = build_array(py_state);
                    if (!py_out) goto error;
                } else {
                    py_out = Py_None;
                }
                break;
            default:
                py_out = (requested_n_rows == 1) ?
                         PyList_GetItem(py_state->py_rows, 0) : py_state->py_rows;
            }
            Py_XINCREF(py_out);
        }
    }
    else {
        switch (py_state->options.results_type) {
        case RESULTS_TYPE_ARRAY:
        case RESULTS_TYPE_DATAFRAME:
        case RESULTS_TYPE_ARROW:
            if (py_state->buffer) {
                py_out = build_array(py_state);
                if (!py_out) goto error;
            } else {
                py_out = Py_None;
            }
            break;
        default:
            py_out = py_state->py_rows;
        }
        Py_INCREF(py_out);

        PyObject *py_n_rows = PyLong_FromSsize_t(py_state->n_rows);
        PyObject_SetAttr(py_res, PyStr.affected_rows, (py_n_rows) ? py_n_rows : Py_None);
        Py_XDECREF(py_n_rows);

        if (py_state->is_eof) {
            PyObject_DelAttr(py_res, PyStr._state);
            Py_CLEAR(py_state);
        }
    }

    PyObject_SetAttr(py_res, PyStr.rows, py_out);

    return py_out;

error:
    goto exit;
}

static PyMethodDef PyMySQLAccelMethods[] = {
    {"read_rowdata_packet", (PyCFunction)read_rowdata_packet, METH_VARARGS | METH_KEYWORDS, "PyMySQL row data packet reader"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef _singlestoredb_accelmodule = {
    PyModuleDef_HEAD_INIT,
    "_singlestoredb_accel",
    "PyMySQL row data packet reader accelerator",
    -1,
    PyMySQLAccelMethods
};

PyMODINIT_FUNC PyInit__singlestoredb_accel(void) {
#ifndef Py_LIMITED_API
    PyDateTime_IMPORT;
#endif

    StateType = (PyTypeObject*)PyType_FromSpec(&StateType_spec);
    if (StateType == NULL || PyType_Ready(StateType) < 0) {
        return NULL;
    }

    ArrayType = (PyTypeObject*)PyType_FromSpec(&ArrayType_spec);
    if (ArrayType == NULL || PyType_Ready(ArrayType) < 0) {
        return NULL;
    }

    // Populate ints
    for (int i = 0; i < 62; i++) {
        PyInts[i] = PyLong_FromLong(i);
    }

    PyStr.unbuffered_active = PyUnicode_FromString("unbuffered_active");
    PyStr._state = PyUnicode_FromString("_state");
    PyStr.affected_rows = PyUnicode_FromString("affected_rows");
    PyStr.warning_count = PyUnicode_FromString("warning_count");
    PyStr.connection = PyUnicode_FromString("connection");
    PyStr.has_next = PyUnicode_FromString("has_next");
    PyStr.options = PyUnicode_FromString("options");
    PyStr.Decimal = PyUnicode_FromString("Decimal");
    PyStr.date = PyUnicode_FromString("date");
    PyStr.time = PyUnicode_FromString("time");
    PyStr.timedelta = PyUnicode_FromString("timedelta");
    PyStr.datetime = PyUnicode_FromString("datetime");
    PyStr.loads = PyUnicode_FromString("loads");
    PyStr.field_count = PyUnicode_FromString("field_count");
    PyStr.converters = PyUnicode_FromString("converters");
    PyStr.fields = PyUnicode_FromString("fields");
    PyStr.flags = PyUnicode_FromString("flags");
    PyStr.scale = PyUnicode_FromString("scale");
    PyStr.type_code = PyUnicode_FromString("type_code");
    PyStr.name = PyUnicode_FromString("name");
    PyStr.table_name = PyUnicode_FromString("table_name");
    PyStr._sock = PyUnicode_FromString("_sock");
    PyStr.settimeout = PyUnicode_FromString("settimeout");
    PyStr._read_timeout = PyUnicode_FromString("_read_timeout");
    PyStr._rfile = PyUnicode_FromString("_rfile");
    PyStr.read = PyUnicode_FromString("read");
    PyStr.x_errno = PyUnicode_FromString("errno");
    PyStr._result = PyUnicode_FromString("_result");
    PyStr._next_seq_id = PyUnicode_FromString("_next_seq_id");
    PyStr.rows = PyUnicode_FromString("rows");
    PyStr.NaT = PyUnicode_FromString("NaT");
    PyStr.table = PyUnicode_FromString("table");
    PyStr.DataFrame = PyUnicode_FromString("DataFrame");
    PyStr.array = PyUnicode_FromString("array");
    PyStr.column_stack = PyUnicode_FromString("column_stack");
    PyStr.O = PyUnicode_FromString("|O");
    PyStr.u1 = PyUnicode_FromString("<u1");
    PyStr.u2 = PyUnicode_FromString("<u2");
    PyStr.u4 = PyUnicode_FromString("<u4");
    PyStr.u8 = PyUnicode_FromString("<u8");
    PyStr.i1 = PyUnicode_FromString("<i1");
    PyStr.i2 = PyUnicode_FromString("<i2");
    PyStr.i4 = PyUnicode_FromString("<i4");
    PyStr.i8 = PyUnicode_FromString("<i8");
    PyStr.f4 = PyUnicode_FromString("<f4");
    PyStr.f8 = PyUnicode_FromString("<f8");
    PyStr.datetime_ns = PyUnicode_FromString("<datetime64[ns]");
    PyStr.shape = PyUnicode_FromString("shape");
    PyStr.typestr = PyUnicode_FromString("typestr");
    PyStr.data = PyUnicode_FromString("data");
    PyStr.copy = PyUnicode_FromString("copy");
    PyStr.core = PyUnicode_FromString("core");
    PyStr.records = PyUnicode_FromString("records");
    PyStr.fromarrays = PyUnicode_FromString("fromarrays");
    PyStr.names = PyUnicode_FromString("names");

    PyMod.decimal = PyImport_ImportModule("decimal");
    if (!PyMod.decimal) goto error;
    PyMod.datetime = PyImport_ImportModule("datetime");
    if (!PyMod.datetime) goto error;
    PyMod.json = PyImport_ImportModule("json");
    if (!PyMod.json) goto error;

    PyFunc.decimal_Decimal = PyObject_GetAttr(PyMod.decimal, PyStr.Decimal);
    if (!PyFunc.decimal_Decimal) goto error;
    PyFunc.datetime_date = PyObject_GetAttr(PyMod.datetime, PyStr.date);
    if (!PyFunc.datetime_date) goto error;
    PyFunc.datetime_timedelta = PyObject_GetAttr(PyMod.datetime, PyStr.timedelta);
    if (!PyFunc.datetime_timedelta) goto error;
    PyFunc.datetime_time = PyObject_GetAttr(PyMod.datetime, PyStr.time);
    if (!PyFunc.datetime_time) goto error;
    PyFunc.datetime_datetime = PyObject_GetAttr(PyMod.datetime, PyStr.datetime);
    if (!PyFunc.datetime_datetime) goto error;
    PyFunc.json_loads = PyObject_GetAttr(PyMod.json, PyStr.loads);
    if (!PyFunc.json_loads) goto error;

    return PyModule_Create(&_singlestoredb_accelmodule);

error:
    return NULL;
}
