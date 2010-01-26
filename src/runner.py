import os, sys

from raccoon import argsToKw, createApp

DEFAULT_cmd_usage = 'python raccoon.py -l [log.config] -r -d [debug.pkl] -x -s server.cfg -p path -m store.nt -a config.py '
cmd_usage = '''
-h this help message
-l [log.config] specify a config file for logging
-r record requests (ctrl-c to stop recording) 
-d [debug.pkl]: debug mode (replay the requests saved in debug.pkl)
-x exit after executing config specific cmd arguments
-p specify the path (overrides RACCOONPATH env. variable)
-m [store.nt] load the RDF model
   (default model supports .rdf, .nt, .mk)
-a config.py run the application specified
'''

def parse_args(argv=sys.argv[1:], out=sys.stdout):
    "parse cmd args and return vars suitable for passing to run"
    vars = {}
    try:
        eatNext = False
        mainArgs, rootArgs, configArgs = [], [], []
        for i in range(len(argv)):
            if argv[i] == '-a':
                rootArgs += argv[i:i+2]
                configArgs += argv[i+2:]
                break
            if argv[i] in ['-d', '-r', '-x', '-s', '-l', '-h', '--help'
                           ] or (eatNext and argv[i][0] != '-'):
                eatNext = argv[i] in ['-d', '-s', '-l']
                mainArgs.append( argv[i] )
            else:
                rootArgs.append( argv[i] )

        if '-l' in mainArgs:
            try:
                logConfig=mainArgs[mainArgs.index("-l")+1]
                if logConfig[0] == '-':
                    raise ValueError
            except (IndexError, ValueError):
                logConfig = 'log.config'
            if not os.path.exists(logConfig):
                raise CmdArgError("%s not found" % logConfig)

            vars['LOG_CONFIG'] = logConfig

        vars.update(argsToKw(rootArgs, DEFAULT_cmd_usage))
        vars['argsForConfig'] = configArgs
        #print 'ma', mainArgs
        if '-h' in mainArgs or '--help' in mainArgs:
            raise CmdArgError('')

        if '-d' in mainArgs:
            try:
                debugFileName=mainArgs[mainArgs.index("-d")+1]
                if debugFileName[0] == '-':
                    raise ValueError
            except (IndexError, ValueError):
                debugFileName = 'debug-wiki.pkl'
            vars['DEBUG_FILENAME'] = debugFileName

        else:
            if '-r' in mainArgs:
                vars['RECORD_REQUESTS'] = True

            #if -x (execute cmdline and exit) we're done
            if '-x' in mainArgs:
                vars['EXEC_CMD_AND_EXIT'] = True

    except (CmdArgError), e:
        print>>out, e
        print>>out, 'usage:'
        print>>out, DEFAULT_cmd_usage +'[config specific options]'
        print>>out, cmd_usage

    return vars

#XXX clean up args and implement this as the doc says
def main(argv=sys.argv[1:], out=sys.stdout):
    '''
    usage app-config.py [options]
    Any appconfig variables can be passed as an command line option 
    and will override the config value set in the app.
    For convenience, short alternative are available:

    -l [log.config] LOG_CONFIG specify a config file for logging
    -x EXEC_CMD_AND_EXIT exit after executing config specific cmd arguments
    -m [store.json] SOURCE (connect to/load the store)
    -r RECORD_REQUESTS record requests (ctrl-c to stop recording) 
    -d [debug.pkl] DEBUG_FILENAME debug mode (replay the requests saved in debug.pkl)    
    '''
    # mimics behavior of old main(), not really used anywhere
    vars = parse_args(argv, out)
    createApp(**vars).run(out=out)
    return 0


if __name__ == '__main__':
    sys.exit(main())
