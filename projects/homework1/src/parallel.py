import multiprocessing
import logging
import traceback
import functools
import numpy as np
import pandas as pd


class WorkerKiller(object):
    def __init__(self):
        pass


class QueuedMap(object):
    def __init__(self, callable_var, list_var, n_jobs=-1):

        self.callable_obj = QueueCallable(callable_var).run
        self.list_var = list_var
        self.n_jobs = n_jobs

    def run(self):
        deltaMat = [None]*len(self.list_var)

        if self.n_jobs == -1:
            num_processors = multiprocessing.cpu_count()
        else:
            num_processors = self.n_jobs

        tasks = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()

        logging.debug('Creating %d processes...' % num_processors)

        processors = [multiprocessing.Process(target=self.callable_obj, args=(tasks,results))
                      for i in xrange(num_processors)]

        for processor in processors:
            logging.debug('starting %s' % processor.name)
            processor.start()

        for ind, item in enumerate(self.list_var):
            tasks.put((ind, item))

        for i in xrange(num_processors):
            tasks.put(WorkerKiller())

        tasks.join()

        for i in range(len(deltaMat)):
            result = results.get()
            deltaMat[result[0]] = result[1]

        for processor in processors:
            logging.debug('stopping %s' % processor.name)
            processor.terminate()

        return deltaMat

def trace_unhandled_exceptions(func):
    @functools.wraps(func)
    def wrapped_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            print 'Exception in '+func.__name__
            traceback.print_exc()
    return wrapped_func

class QueueCallable(object):
    def __init__(self, callable_var):
        self.callable_var = callable_var

    @trace_unhandled_exceptions
    def run(self, taskQueue, resultQueue):
        while True:
            task = taskQueue.get()
            if isinstance(task, WorkerKiller):
                logging.debug('Exiting process...')
                taskQueue.task_done()
                break

            ind = task[0]
            res = self.callable_var(task[1])

            taskQueue.task_done()
            resultQueue.put((ind, res))


def apply_parallel(callable_var, gb_obj, n_jobs=-1):
    print('Extracting parallel jobs')
    obj_array = np.array([obj for obj in gb_obj])

    indices_store = obj_array[:, 0]

    mapper = QueuedMap(callable_var, obj_array[:, 1].tolist(), n_jobs)

    print('Running parallel jobs')
    results = mapper.run()

    if len(indices_store[0]) > 1:
        indices_store = pd.MultiIndex.from_tuples(indices_store)

    return pd.Series(results, index=indices_store)
