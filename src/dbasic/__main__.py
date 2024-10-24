from dbasic.repl import print_prompt, read_input


def main():
    while True:
        print_prompt()
        _input = read_input()

        if _input == ".exit":
            return
        else:
            print(f"Unrecognised command {_input}")


if __name__ == "__main__":
    main()
