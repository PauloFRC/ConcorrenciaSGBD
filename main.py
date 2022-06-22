# Paulo Ricardo Fernandes
# José Lucas

from queue import Queue

class Tr:
    def __init__(self, id, ts):
        self.id = id
        self.ts = ts
        self.state = 0  # 0->active 1->commited 2->aborted

class Tr_Manager:
    def __init__(self):
        self.trs = {}


class Lock_Manager:
    def __init__(self):
        self.lock_table = {}
        # x: ['S', [1,2]]
        self.wait_q = {}
        self.wait_w = {}

    # adiciona shared lock no item d para a transação tr caso não exista na lock_table
    def ls(self, tr, table, tr_man):
        # se tabela não tiver lockada
        if self.lock_table.get(table) == None:
            self.lock_table[table] = ['S', [tr]]
            shared_lock_op = Operation(
                tr=tr, table=table, action='sl')
            return shared_lock_op
        # se tabela tiver shared lock e a transação atual não estiver lockada
        elif self.lock_table[table][0] == 'S' and tr not in self.lock_table[table][1]:
            self.lock_table[table][1].append(tr)
            shared_lock_op = Operation(
                tr=tr, table=table, action='sl')
            return shared_lock_op
        # se a tabela for exclusive lock e não for lockado da mesma transação
        elif self.lock_table[table][0] == 'X' and self.lock_table[table][1] != [tr]:
            '''if self.wait_q.get(table) == None:
                self.wait_q[table] = Queue()
            self.wait_q[table].put((tr, 'S'))'''
            if tr_man[tr].ts < tr_man[self.lock_table[table][1][0]].ts:
                return 'wound'
            return 'wait'
        # se já houver shared ou exclusive lock da mesma transação
        else:
            return None

    # adiciona exclusive lock no item d para a transação tr caso não exista na lock_table
    def lx(self, tr, table, tr_man):
        # se tabela não tiver lockada
        if self.lock_table.get(table) == None:
            self.lock_table[table] = ['X', [tr]]
            exclusive_lock_op = Operation(
                tr=tr, table=table, action='xl')
            return exclusive_lock_op
        # se tabela tiver shared lock da mesma transação, aumente o nível para exclusivo
        elif self.lock_table[table][0] == 'S' and self.lock_table[table][1] == [tr]:
            self.lock_table[table] = ['X', [tr]]
            exclusive_lock_op = Operation(
                tr=tr, table=table, action='xl')
            return exclusive_lock_op
        # se a tabela for exclusive lock e não for lockado da mesma transação ou for shared lock mas houver outra transação
        # elif self.lock_table[table][0] == 'X' and self.lock_table[table][1] != [tr]:
        elif self.lock_table[table][1] != [tr]:
            if tr_man[tr].ts < tr_man[self.lock_table[table][1][0]].ts:
                return 'wound'
            return 'wait'
        # se já houver exclusive lock da mesma transação
        else:
            return None

    # apaga lock do item d para a transação tr
    def u(self, tr, table):
        transactions = self.lock_table[table][1]
        if tr in transactions:
            transactions.remove(tr)
            if len(transactions) == 0:
                self.lock_table.pop(table)
            unlock_op = Operation(
                    tr=tr, table=table, action='u')
            return unlock_op
        return False
            

    def __str__(self):
        return str(self.lock_table)


class Operation:
    def __init__(self, tr, action, table):
        self.tr = tr
        self.action = action
        self.table = table

    def __str__(self):
        return f'Ação: {self.action}, Transação: {self.tr}, Tabela: {self.table}'


class Scheduler:
    def __init__(self):
        self.operations = Queue()
        self.tr_manager = Tr_Manager()
        self.lock_manager = Lock_Manager()
        self.final_history = []
        self.wait_transactions = []
        self.wait_operations = []
        self.wound_operations = {}

    def run(self, history):
        self.parser(history)
        self.execute_operations()
        #for q in self.lock_manager.wait_q.keys():
        #    print(f'{q}: {list(self.lock_manager.wait_q[q].queue)}')
        print()
        for action in self.final_history:
            print(action)

    def parser(self, history):
        try:
            actions = history.split(')')[:-1]
            for action in actions:
                index_brack = action.find('(')
                if action[:2] == 'BT':
                    transaction = action[3:]
                    operation = Operation(
                        tr=transaction, action='bt', table=None)
                    self.operations.put(operation)
                elif action[0] == 'r':
                    transaction = action[1:index_brack]
                    table = action[index_brack+1:]
                    operation = Operation(
                        tr=transaction, action='r', table=table)
                    self.operations.put(operation)
                elif action[0] == 'w':
                    transaction = action[1:index_brack]
                    table = action[index_brack+1:]
                    operation = Operation(
                        tr=transaction, action='w', table=table)
                    self.operations.put(operation)
                elif action[0] == 'C':
                    transaction = action[2:]
                    operation = Operation(
                        tr=transaction, action='c', table=None)
                    self.operations.put(operation)
        except:
            print('Erro no input')

    def is_active(self, tr):
        transaction = self.tr_manager.trs[tr]
        if transaction.state != 0:
            print('Operação em transação não ativa ignorada')
            return False
        return True

    def execute_operations(self):
        timestamp = 0
        while not self.operations.empty():
            op = self.operations.get()

            if op.tr in self.wait_transactions:
                self.wait_operations.append(op)
                continue

            if op.action == 'bt':
                tr = Tr(id=op.tr, ts=timestamp)
                self.tr_manager.trs[op.tr] = tr
                timestamp += 1
                self.final_history.append(op)

            elif op.action == 'r':
                # pega transação no transaction manager e checa se está ativa
                if not self.is_active(op.tr):
                    continue
                # adiciona Shared Lock se possível
                lock = self.lock_manager.ls(tr=op.tr, table=op.table, tr_man=self.tr_manager.trs)
                if lock == None:
                    self.final_history.append(op)
                elif lock == 'wait':
                    print('wait')
                    # coloca operação na lista de espera para quando table for liberada
                    if self.lock_manager.wait_q.get(op.table) == None:
                        self.lock_manager.wait_q[op.table] = []
                    self.lock_manager.wait_q[op.table].append(op.tr)

                    self.wait_transactions.append(op.tr)
                    self.wait_operations.append(op)

                elif lock == 'wound':
                    print('wound')

                    if self.lock_manager.wait_w.get(op.table) == None:
                        self.lock_manager.wait_w[op.table] = []
                    self.lock_manager.wait_w[op.table].append(op.tr)

                    final_history_copy = list(self.final_history)
                    operations_queue = list(self.operations.queue)
                    for operation in self.final_history + [op] + list(self.operations.queue):
                        if operation in self.final_history and operation.tr == op.tr:
                            final_history_copy.remove(operation)
                        elif operation in self.operations.queue and operation.tr == op.tr:
                            operations_queue.remove(operation) 
                        if operation.action in ('bt', 'r', 'w', 'c') and operation.tr == op.tr:
                            if self.wound_operations.get(operation.tr) == None:
                                self.wound_operations[operation.tr] = []
                            print('op adicionado', operation)
                            self.wound_operations[operation.tr].append(operation)
                        
                        new_queue = Queue()
                        for ope in operations_queue:
                            new_queue.put(ope)
                    
                    self.final_history = final_history_copy
                    


                else:
                    self.final_history.append(lock)
                    self.final_history.append(op)

            elif op.action == 'w':
                # pega transação no transaction manager e checa se está ativa
                if not self.is_active(op.tr):
                    continue
                # adiciona exclusive lock se possível
                lock = self.lock_manager.lx(tr=op.tr, table=op.table, tr_man=self.tr_manager.trs)
                if lock == None:
                    self.final_history.append(op)
                elif lock == 'wait':
                    print('wait')

                    if self.lock_manager.wait_q.get(op.table) == None:
                        self.lock_manager.wait_q[op.table] = []
                    self.lock_manager.wait_q[op.table].append(op.tr)

                    self.wait_transactions.append(op.tr)
                    self.wait_operations.append(op)

                    # coloca operação na lista de espera para quando table for liberada
                elif lock == 'wound':
                    print('wound')

                    if self.lock_manager.wait_w.get(op.table) == None:
                        self.lock_manager.wait_w[op.table] = []
                    self.lock_manager.wait_w[op.table].append(op.tr)

                    final_history_copy = list(self.final_history)
                    operations_queue = list(self.operations.queue)
                    for operation in self.final_history + [op] + list(self.operations.queue):
                        if operation in self.final_history and operation.tr == op.tr:
                            final_history_copy.remove(operation)
                        elif operation in self.operations.queue and operation.tr == op.tr:
                            operations_queue.remove(operation) 
                        if operation.action in ('bt', 'r', 'w', 'c') and operation.tr == op.tr:
                            if self.wound_operations.get(operation.tr) == None:
                                self.wound_operations[operation.tr] = []
                            print('op adicionado', operation)
                            self.wound_operations[operation.tr].append(operation)
                        
                        new_queue = Queue()
                        for ope in operations_queue:
                            new_queue.put(ope)
                    
                    self.final_history = final_history_copy


                else:
                    self.final_history.append(lock)
                    self.final_history.append(op)

            # coloca transação como commitada e faz todos os unlocks
            elif op.action == 'c':
                self.tr_manager.trs[op.tr].state = 1
                self.final_history.append(op)

                for table in list(self.lock_manager.lock_table.keys()):
                    unlock = self.lock_manager.u(tr=op.tr, table=table)
                    if unlock != False:

                        wait_list = self.lock_manager.wait_q.get(table)
                        if wait_list:
                            new_operations = []
                            copy_wait_operations = list(self.wait_operations)
                            for operation in self.wait_operations:
                                if operation.tr in wait_list:
                                    new_operations.append(operation)
                                    copy_wait_operations.remove(operation)
                                if operation.tr in self.wait_transactions:
                                    self.wait_transactions.remove(operation.tr)
                            wait_list_copy = list(wait_list)
                            for operation in self.wait_operations:
                                if operation.tr in wait_list_copy:
                                    wait_list_copy.remove(operation.tr)

                            self.lock_manager.wait_q[table] = wait_list_copy
                            if len(wait_list_copy)==0:
                                self.lock_manager.wait_q.pop(table)
                            self.wait_operations = copy_wait_operations


                            new_queue = Queue()
                            for operation in new_operations + list(self.operations.queue):
                                new_queue.put(operation)
                            #for oper in new_queue.queue:
                            #    print(oper)
                            self.operations = new_queue
                        
                        wound_list = self.lock_manager.wait_w.get(table)
                        if wound_list:
                            for transaction in wound_list:
                                wound_operations_copy = list(self.wound_operations[transaction])
                                tr_operations = self.wound_operations[transaction]

                                for operation in tr_operations:
                                    print('operacao adicionada final', operation)
                                    self.operations.put(operation)
                                    wound_operations_copy.remove(operation)
                                self.wound_operations[transaction] = wound_operations_copy

                            self.lock_manager.wait_w.pop(table)
                            

                                
                                
                        self.final_history.append(unlock)



sc = Scheduler()
#sc.run('BT(2)BT(1)w1(x)r2(x)C(1)C(2)')
#sc.run('BT(3)BT(2)BT(1)w1(x)w2(x)w3(x)C(1)C(2)C(3)')
sc.run('BT(1)w1(z)r1(x)BT(2)w2(z)r2(y)r1(y)C(1)w2(x)w2(x)C(2)r2(x)')