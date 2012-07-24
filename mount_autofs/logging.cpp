#include <ctime>
#include <cstdio>
#include <cstdarg>
#include <cstring>

#include <syslog.h>

#include "logging.h"

static FILE *g_DebugFile = NULL;

void _debug_printf(const char *func, int line, const char *format, ...) {
    if (g_DebugFile == NULL) {
        //syslog(LOG_ERR, "opening debug log");
        g_DebugFile = fopen("debug.log", "w");
    }

    char tbuf[64];
    time_t tmt = time(NULL);
    tm *tm = localtime(&tmt);
    strftime(tbuf, 63, "%H:%M:%S ", tm);
    fwrite(tbuf, 1, strlen(tbuf), g_DebugFile);
    fprintf(g_DebugFile, "%s:%d: ", func, line);

    va_list args;
    va_start(args, format);
    vfprintf(g_DebugFile, format, args);
    va_end(args);
    fflush(g_DebugFile);
}

void _syslog_error(const char *what, const char *func, int line) {
    char buffer[512];
    strcpy(buffer, what);
    strcat(buffer, " (in %s:%d)");
    syslog(LOG_ERR, buffer, func, line);
    DBPRINTF("%s\n", what);
}
