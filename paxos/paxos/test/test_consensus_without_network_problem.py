from typing import List
import os

# import re
import random

import subprocess
import time


def kill_servers():
    os.system("kill $(pidof svr)")


def run_server(member_id: int, members: List[str]):
    cmd = f"./svr {member_id} {' '.join(members)} &"
    res = os.system(cmd)
    if res != 0:
        raise Exception(f"{res}:{cmd}")


# {{ 0 0} LEARNT}
# {{:22222 0 33} LEARNT}
# @dataclass
# class Response:
#     addr:str
#     ver:str


def run_cli(addr: str, arg: str) -> tuple[int, str, str]:
    cmd = f"./cli {addr}".split()

    process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = process.communicate(input=arg)

    # if process.returncode != 0:
    #     raise Exception(stderr)
    # reg = re.compile(r"\{\{(:\d+) (\d+) (\d+)\} (\w+)\}")
    # print(stdout)
    # # reg = re.compile(r"\{([^}])*\}")
    # aa = reg.findall(stdout)
    # print(aa)
    return process.returncode, stdout, stderr


def main():
    port_base = 22220
    num_nodes = 11
    addr = []
    kill_servers()
    for port in range(port_base, port_base + num_nodes):
        addr.append(f":{port}")
    for i, p in enumerate(addr):
        run_server(i, addr)
    numbers = []
    # res = []
    time.sleep(3)
    for _ in range(100):
        numbers.append(random.randint(1, 1000))
        arg = f"set {numbers[-1]}"
        i = random.randint(0, num_nodes - 1)
        ret = run_cli(addr[i], arg)
        if ret[0] != 0:
            raise Exception(str(ret))
        time.sleep(1)
        # tmp = []
        first = None
        for j in range(num_nodes):
            rep = run_cli(addr[j], "get")
            if j == 0:
                first = rep
            else:
                if first != rep:
                    raise Exception("{} {} {}".format(numbers[i], first, rep))


if __name__ == "__main__":
    main()
