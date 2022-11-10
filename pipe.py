from typing import Iterable, Generator, Tuple, List, Callable
import functools
import itertools

class Pipe:

    def __init__(self):
        # self.is_batch = is_batch TODO improvement
        self.node_list: List[Callable] = []
        self.blackboard = {}
        self._was_called = False


    def __call__(self, *args_input, **kwargs_input):
        output = self.node_list[0](*args_input, **kwargs_input)

        for node in self.node_list[1:]:
            output = node(output)

        return output


    def get_blackboard(self, input=None):
        if not self._was_called:
            result = self.__call__(input)
            if isinstance(result, Generator):
               result = list(result) # force generator execution
               # TODO: run inner generators inside result.

        return self.blackboard


    # i node - Simplest node, one input to one output
    def i_n(self, name: str, worker: Callable, *params, to_blackboard=False, **kwparams) -> "Pipe":
        @functools.wraps(worker)
        def wrapper(*args, **kwargs):
            try:
                args = args + params
                kwargs = kwargs | kwparams

                output = worker(*args, **kwargs)
                if to_blackboard:
                    self.blackboard[name] = output
                return output
            except:
                print(f"Error in i-node {name}")
                raise

        self.node_list.append(wrapper)

        return self


    @staticmethod
    def _tee_if_forking(wrapper: Callable, forks: int):
        def wrapper_tee(*args, **kwargs):
            return itertools.tee(wrapper(*args, **kwargs),forks)
        tee_wrapper = wrapper
        if forks>0:
            tee_wrapper = wrapper_tee
        return tee_wrapper


    # x node: multiple inputs, to multiple outputs.
    # If only one worker is given could be see as batch i node
    def x_n(self, name: str, *workers: Callable, forks: int=0):

        def execute(parallel_input, worker, count=None):
            if isinstance(worker, Callable):
                output = worker(parallel_input)
                if isinstance(worker, type(self)):
                    self.blackboard.update(worker.blackboard)
                return output
            elif isinstance(worker, Iterable):
                name_w, worker, params, to_blackboard, kwparams = worker
                output = worker(parallel_input, *params, **kwparams)
                if isinstance(worker, type(self)) and to_blackboard:
                    self.blackboard.update(worker.blackboard)
                elif to_blackboard:
                    if count is not None:
                        name_w = f"{name_w}_{count}"
                    self.blackboard[name_w] = output
                return output
            else:
                raise ValueError("worker should be Callable a tuple -> (Callable, tuple, dict)")

        def wrapper(parallel_inputs: Generator) -> Generator:
            try:
                parallel_count = 0
                if len(workers)==1:
                    for count, parallel_input in enumerate(parallel_inputs):
                        parallel_count = count
                        yield execute(parallel_input, workers[0], count)
                else:
                    if len(workers)!=len(parallel_inputs):
                        raise ValueError("Whe more than one worker are given, number should be the same than of input data")
                    for count, (parallel_input, worker) in enumerate(zip(parallel_inputs, workers)):
                        parallel_count = count
                        yield execute(parallel_input, worker)
            except:
                print(f"Error in x-node {name}, for parallel element {parallel_count}")
                raise

        wrapper_to_append = Pipe._tee_if_forking(wrapper, forks)

        self.node_list.append(wrapper_to_append)

        return self


    # λ node - One input unfolded to multiple outputs
    # noinspection NonAsciiCharacters
    def λ_n(self, name: str, *unfolding_workers: Callable, forks: int=0) -> "Pipe":
        # TODO: might could be a good idea to separete forks functionality, somethind like λ_tee
        def wrapper(*args, **kwargs) -> Generator:
            try:
                for count, worker in enumerate(unfolding_workers):
                    folded_count = count
                    if isinstance(worker, Callable):
                        output = worker(*args, **kwargs)
                        if isinstance(worker, type(self)):
                            self.blackboard.update(worker.blackboard)
                        yield output
                    elif isinstance(worker, Iterable):
                        name_w, worker, params, to_blackboard, kwparams = worker
                        args_ = args + params
                        kwargs_ = kwargs | kwparams
                        output = worker(*args_, **kwargs_)
                        if isinstance(worker, type(self)) and to_blackboard:
                            self.blackboard.update(worker.blackboard)
                        elif to_blackboard:
                            self.blackboard[name_w] = output
                        yield output
                    else:
                        raise ValueError("unfolding_worker, should be Callable a tuple -> (Callable, tuple, dict)")
            except:
                print(f"Error in lambda-node {name}, for unfold number {folded_count}")
                raise

            wrapper_to_append = Pipe._tee_if_forking(wrapper, forks)

            self.node_list.append(wrapper_to_append)

        return self


    # y node - Multiple inputs folded to one output
    def y_n(self, name: str, folding_worker: Callable, *params, to_blackboard=False, **kwparams) -> "Pipe":
        @functools.wraps(folding_worker)
        def wrapper(to_fold: Generator):
            try:

                output = folding_worker(to_fold, *params, **kwparams)

                if to_blackboard:
                    self.blackboard[name] = output

                return output
            except:
                print(f"Error in upsilon-node {name}")
                raise

        self.node_list.append(wrapper)

        return self



    # y node flatten - Multiple inputs folded to one output, this case flatten all elements to a list
    def y_n_flatten(self, name: str) -> "Pipe":

        def flatten(to_fold: Generator):
            try:
                return list(to_fold)
            except:
                print(f"Error in upsilon-flatten-node {name}")
                raise

        self.node_list.append(flatten)

        return self


    def  i_n_to_blackboard_and_continue(self, name: str, to_save: Callable, *args, **kwargs):
        return self.λ_n(f"save_and_continue_{name}",
                        Pipe.w(name, to_save, *args, to_blackboard=True, **kwargs),
                        Pipe.w(f"sub_continue_{name}", Pipe.identity)) \
                   .y_n(f"continue_{name}", Pipe.y_c_get_one)


    @classmethod
    def w(cls, name: str, worker: Callable, *params, to_blackboard=False, **kwparams):
        return name, worker, params, to_blackboard, kwparams


    @classmethod
    def identity(cls, input):
        return input


    @classmethod
    def y_c_get_one(cls, to_combine: Generator, index_to_get=-1):
        generations_list = list(to_combine)
        return generations_list[index_to_get]


    @classmethod
    def auto_fold(cls, folding: Callable, *params, **kwparams):
        @functools.wraps(folding)
        def wrapper(to_fold, *args, **kwargs):
            args = args + params
            kwargs = kwargs | kwparams
            return  folding(*to_fold,*args, **kwargs)
        return wrapper


# usage example
def example_test():
    pipe = Pipe().i_n("paso1", lambda x: x + 1) \
                 .i_n("paso2", lambda x: x + 5) \
                 .λ_n("paso3", (lambda x: x + 2), (lambda x: x + 4)) \
                 .y_n("paso4", lambda x: list(x))
    return pipe(0) # call: pipe(0), would return [8, 10], pipe(7) should return [15,17]




