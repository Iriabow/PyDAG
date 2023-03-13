import sys, os
from typing import Iterable, Generator, Tuple, List, Callable
import functools
import itertools
import logging


class Failure:
    def __init__(self, circuit_breaking_is_active):
        self.circuit_breaking_is_active = circuit_breaking_is_active

    def __call__(self, exception: Exception = ValueError("Execution failed")):
        if not self.circuit_breaking_is_active:
            raise exception
        return self


class Pipe:

    # TODO: problems: reuse common nodes, common pipes chaining, without changing node_list...
    def __init__(self, logger=logging, failure_value=None, circuit_breaking_is_active=True, pipe_name=None):
        self.circuit_breaking_is_active = circuit_breaking_is_active
        # self.is_batch = is_batch TODO improvement
        # TODO: bug: node_composition, would be overwritten if pipes are compose through method operator.
        self.node_composition: Callable = None
        # TODO: bug: blackboard would be overwritten if a pipe is called multiple times.
        self.blackboard = {}
        # TODO: what happens, when the same pipe is called with different inputs, inputs also should cached.
        self._was_called = False
        self.logger = logger
        self.failure_value = failure_value
        self.failure = Failure(circuit_breaking_is_active)
        self.pipe_name = pipe_name


    def __call__(self, *args_input, **kwargs_input):
        output = self.node_composition(*args_input, **kwargs_input)
        # TODO: check if output is a generator and force generation.
        self._was_called = True

        return output


    def get_blackboard(self, input=None):
        if not self._was_called:
            result = self.__call__(input)
            if isinstance(result, Generator):
               result = list(result) # force generator execution
               # TODO: run inner generators inside result.

        return self.blackboard.copy()


    # i node - Simplest node, one input to one output
    def i_n(self, name: str, worker: Callable, *params, to_blackboard=False, debugging=False, **kwparams) -> "Pipe":
        @functools.wraps(worker)
        def wrapper(*args, **kwargs):
            args = args + params
            kwargs = kwargs | kwparams
            output = self._execute("i_n", name, worker, *args, name=name, to_blackboard=to_blackboard, **kwargs)
            Pipe._debug(debugging, name, output, *args, **kwargs)
            return output

        self.node_composition = Pipe._compose_functions(self.node_composition, wrapper)

        return self


    @staticmethod
    def _tee_if_forking(wrapper: Callable, forks: int):
        def wrapper_tee(*args, **kwargs):
            return itertools.tee(wrapper(*args, **kwargs), forks)
        tee_wrapper = wrapper
        if forks>0:
            tee_wrapper = wrapper_tee
        return tee_wrapper


    # x node: multiple inputs, to multiple outputs.
    # If only one worker is given could be see as batch i node
    def x_n(self, name: str, *workers: Callable, forks: int=0, debugging=False):
        def wrapper(parallel_inputs: Iterable) -> Generator:
            if len(workers)==1:
                for count, parallel_input in enumerate(parallel_inputs):
                    output = self._execute("x_n", name, workers[0], parallel_input, count=count, step=count)
                    Pipe._debug(debugging, name, output, parallel_input)
                    yield output
            else:
                if len(workers)!=len(parallel_inputs):
                    raise ValueError("When more than one worker are given, number should be the same than of input data")
                for count, (parallel_input, worker) in enumerate(zip(parallel_inputs, workers)):
                    output = self._execute("x_n", name, worker, parallel_input, step=count)
                    Pipe._debug(debugging, name, output, parallel_input)
                    yield output

        wrapper_to_compose = Pipe._tee_if_forking(wrapper, forks)
        self.node_composition = Pipe._compose_functions(self.node_composition, wrapper_to_compose)

        return self


    # λ node - One input unfolded to multiple outputs
    # noinspection NonAsciiCharacters
    def λ_n(self, name: str, *unfolding_workers: Callable, forks: int=0, debugging=False) -> "Pipe":
        # TODO: might could be a good idea to separete forks functionality, somethind like λ_tee
        def wrapper(*args, **kwargs) -> Generator:
            for count, worker in enumerate(unfolding_workers):
                output = self._execute("λ_n", name, worker, *args, step=count, **kwargs)
                Pipe._debug(debugging, name, output, *args, **kwargs)
                yield output

        wrapper_to_compose = Pipe._tee_if_forking(wrapper, forks)
        self.node_composition = Pipe._compose_functions(self.node_composition, wrapper_to_compose)

        return self


    # y node - Multiple inputs folded to one output
    def y_n(self, name: str, folding_worker: Callable, *params, to_blackboard=False, debugging=False, **kwparams) -> "Pipe":

        @functools.wraps(folding_worker)
        def wrapper(to_fold: Iterable):
            output = self._execute("y_n", name,
                                   folding_worker, to_fold, *params, name=name, to_blackboard=to_blackboard, **kwparams)

            Pipe._debug(debugging, name, output, *to_fold)
            return output

        self.node_composition = Pipe._compose_functions(self.node_composition, wrapper)

        return self


    # y node flatten generator - Multiple inputs folded to one output, this case flatten all elements to a list
    def y_n_flatten(self, name: str) -> "Pipe":

        def flatten(to_fold: Iterable):

            return self._failure_controlled_execution("y_n_flatten", name, list, to_fold)

        self.node_composition = Pipe._compose_functions(self.node_composition, flatten)

        return self


    def i_n_do_and_continue(self, name: str, to_save: Callable, *args, to_blackboard=False, debugging=False, **kwargs):

        return self.λ_n(f"save_and_continue_{name}",
                        Pipe.w(name, to_save, *args, to_blackboard=to_blackboard, **kwargs),
                        Pipe.w(f"sub_continue_{name}", Pipe.identity),
                        debugging=debugging) \
                   .y_n(f"continue_{name}", Pipe.y_c_get_one, debugging=debugging)


    @staticmethod
    def w(name: str, worker: Callable, *params, to_blackboard=False, **kwparams):

        return name, worker, params, to_blackboard, kwparams


    @classmethod
    def identity(cls, input):

        return input


    @classmethod
    def y_c_get_one(cls, to_combine: Iterable, index_to_get=-1):

        generations_list = list(to_combine)

        return generations_list[index_to_get]


    @classmethod
    def auto_fold_with(cls, folding: Callable, *params, **kwparams) -> Callable:

        @functools.wraps(folding)
        def wrapper(to_fold, *args, **kwargs):
            args = args + params
            kwargs = kwargs | kwparams

            return folding(*to_fold,*args, **kwargs)

        return wrapper


    @staticmethod
    def _compose_functions(first_step: Callable, second_step: Callable) -> Callable:
        if not first_step: # initially self.node_composition, would be None
            return second_step

        def composition(*args, **kwargs):
            output = first_step(*args, **kwargs)

            return second_step(output)

        return composition


    def _execute(self, node_type, node_name, worker, *args, count=None, name=None, to_blackboard=False, **kwargs):

        def save_to_blackboard(name, output, to_blackboard_w=False):
            if isinstance(worker, type(self)):
                self.blackboard.update(worker.blackboard)
            elif to_blackboard or to_blackboard_w:
                if count is not None:
                    name = f"{name}_{count}"
                if isinstance(output, Failure):
                    output = self.failure_value
                self.blackboard[name] = output

        if isinstance(worker, Callable):
            output = self._failure_controlled_execution(node_type, node_name, worker, *args, **kwargs)
            save_to_blackboard(name, output)

            return output

        elif isinstance(worker, Iterable):
            name_w, worker, params, to_blackboard_w, kwparams = worker
            output = self._failure_controlled_execution(node_type, node_name, worker, *args, *params,**kwargs, **kwparams)

            save_to_blackboard(name_w, output, to_blackboard_w)

            return output

        else:
            raise ValueError("worker should be Callable or a tuple -> (Callable, tuple, dict)")


    def _failure_controlled_execution(self, node_type, node_name, worker: Callable, *args, step=None, **kwargs):
        try:
            # If worker is a Pipe should be called to generate its own failure_values in its dictionary.
            if args and isinstance(args[0], Failure) and not isinstance(worker, type(self)):

                return args[0]

            return worker(*args, **kwargs)

        except Exception as failure:

            if isinstance(worker, Iterable):
                name_w, worker, params, to_blackboard, kwparams = worker
                node_name = f"{node_name} in subnode {name_w}"
            elif step:
                node_name = f"{node_name} in step {step}"

            self.logger.error(f" in {node_type} with name {node_name}:\n {failure}")

            return self.failure(exception=failure)


    @staticmethod
    def _debug(debugging:bool, node_name, output, *args, **kwargs, ):
        if debugging:
            print(f"node: {node_name}")
            print(f"args: {args}")
            print(f"kwargs: {kwargs}")
            print(f"output: {output}")


# usage example
def example_test():
    pipe = Pipe().i_n("paso1", lambda x: x + 1) \
                 .i_n("paso2", lambda x: x + 5) \
                 .λ_n("paso3", (lambda x: x + 2), (lambda x: x + 4)) \
                 .y_n("paso4", lambda x: list(x))
    return pipe(0) # call: pipe(0), would return [8, 10], pipe(7) should return [15,17]




