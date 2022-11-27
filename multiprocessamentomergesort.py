"""
Paulo Ramon Nogueira de Freitas
Universidade Federal do Ceará (UFC)
Com base no código original de Joshua Stough
Da Washington and Lee University

Código desenvolvido como parte da Entraga 01 da
Disciplina de Sistemas Operacionais do Programa de
Mestrado em Computação da (UFC)

Execução Paralela vs Execução Sequencial
Algoritmo Mergesort
Usando multiprocessing Process/Pipe.

$ pytho3 multiprocessamentoMargeSortFinal.py

Veja:
http://docs.python.org/library/multiprocessing.html
http://docs.python.org/py3k/library/multiprocessing.html
"""

from multiprocessing import Process, Pipe
import numpy as np
import time


def main():
    """
    Este é o método principal do programa, onde nós:
    * Pegamos uma lista de um arquivo TXT (devolve_array(TAMANHO_ARQUIVO)).
      sorteado aleatoriamente;
    * Definimos a quantidade de Processos utilizados;
    * Ordenamos a lista de forma sequencial;
    * Ordenamos a lista de forma Paralela;
    * Salvamos as listas ordenadas em Arquivos na Pasta "Arquivos/Ordenados"
    """
    # Tamanho do arquivo a ser utilizado
    TAMANHO_ARQUIVO = '800k'
    # Constante com a quantidade de Interações
    QTD_INTERACOES = 10

    num_thread = int(input("Qual a quantidade de Threads a ser utilizada?\n"))
    # Array para calcular o Tempo Médio de Execução (TME)
    tme = []

    # Parte 1 – Comparação Sequencial - Mesrgesort
    # Utilizando um arquivo aleatório a cada interação
    for i in range(0, 10):
        lista = list(devolve_array(TAMANHO_ARQUIVO))
        ti = time.time()  # Tempo inicial
        lista = mergesort(lista)
        tf = time.time() - ti  # Tempo final

        # Validando a ordenação Inicial
        if not esta_ordenada(lista):
            print('O Margesort Sequential não funcionou :(.')
            return 1

        # Salva a lista ordenada em Arquivo
        cria_arquivo(list(lista), i, "Sequencial", TAMANHO_ARQUIVO)

        # Adiciona o tempo final no vetor para cálculo da média
        tme.append(tf)

    print('Tempos totais de execução Sequencial:')
    print(tme)
    print('Tempo Médio da Primeira Comparação Sequencial Mergesort: %f sec' % np.mean(tme))

    # O uso da CPU é pausado por 3 segundos
    time.sleep(3)

    # Inicializando o Array para cálculo da média do tempo de execução
    tme = []

    # Parte 2 - Comparação Paralelo - Mesrgesort
    # Utilizando um arquivo aleatório a cada interação

    for i in range(QTD_INTERACOES):
        lista = list(devolve_array(TAMANHO_ARQUIVO))
        ti = time.time()  # Tempo inicial

        # Instancia um Processo e envie a lista inteira, junto com um
        # Pipe para que possamos receber sua resposta e uma quantidade
        # de Processos a serem criados
        pconn, cconn = Pipe()
        p = Process(target=merge_sort_parallel, args=(lista, cconn, num_thread))
        p.start()
        lista = pconn.recv()

        # Bloqueia até que haja algo (a lista ordenada) para receber.
        p.join()

        tf = time.time() - ti  # Tempo Final

        # Validando a Ordenação Inicial
        if not esta_ordenada(lista):
            print('O Margesort Sequential não funcionou :(.')
            return 1

        # Salva a lista ordenada em Arquivo
        cria_arquivo(list(lista), i, "Paralelo", TAMANHO_ARQUIVO)

        # Adiciona o tempo final no vetor para cálculo da média
        tme.append(tf)

    print('Tempos totais de execução em Paralelo:')
    print(tme)
    print('Tempo Médio da Comparação Sequencial com Paralelismo Mergesort: %f sec' % (np.mean(tme)))

    time.sleep(3)


# Prepara o Array a partir de um Arquivo de texto aleatorio
def devolve_array(tamanho):
    nome_arquivo = "../Arquivos/" + tamanho + "/arquivo" + str(np.random.randint(0, 10)) + ".txt"
    ref_arquivo = open(nome_arquivo, "r")
    linha = ref_arquivo.readline()
    array = [int(linha)]

    for linha in ref_arquivo:
        array.append(int(linha))

    ref_arquivo.close()

    return array


# Cria um arquivo com a lista ordenada
def cria_arquivo(array, i, tipo, tamanho):
    novo_arquivo = open("../Arquivos/Ordenados/arquivo" + str(i) + "-" + str(tipo) + "-" + str(tamanho) + "-Ordenado.txt", "a")

    for j in range(len(array)):
        novo_arquivo.writelines(str(array[j]) + "\n")

    novo_arquivo.close()

# Mescla duas Listas ordenadamente
def merge(left, right):
    """
    Retorna uma versão mesclada e classificada das duas listas já classificadas.
    """
    ret = []

    """
    A notação de atribuição simples do Python nos permite atribuir a 
    várias variáveis um mesmo valor numa mesma expressão. Para isso, 
    devemos separar as variáveis que receberam o mesmo valor com vírgulas.
    """
    li = ri = 0

    while li < len(left) and ri < len(right):
        if left[li] <= right[ri]:
            ret.append(left[li])
            li += 1
        else:
            ret.append(right[ri])
            ri += 1
    if li == len(left):
        ret.extend(right[ri:])
    else:
        ret.extend(left[li:])
    return ret

# Quebra uma lista em duas e chama recursivamente a merge(left, right)
def mergesort(lista):
    """
    Retorna uma cópia classificada da lista. Observe que isso não altera
    a lista de argumentos.
    """
    if len(lista) <= 1:
        return lista
    ind = len(lista) // 2
    return merge(mergesort(lista[:ind]), mergesort(lista[ind:]))

# Paralelliza o Algoritmo
def merge_sort_parallel(lista, conn, proc_num):
    """
    merg_sort_parallel recebe uma lista, uma conexão Pipe para o pai e procNum.
    Ordena os lados esquerdo e direito em paralelo, então mescla os resultados
    e envia pelo Pipe para o pai.
    """

    # Caso base, este processo é uma folha ou o problema muito pequeno.
    if proc_num <= 0 or len(lista) <= 1:
        conn.send(mergesort(lista))
        conn.close()
        return

    ind = len(lista) // 2

    # Crie processos para ordenar as metades esquerda e direita da lista.

    # Ao criar um processo filho, também criamos um canal para que esse
    # filho comunique a lista ordenada de volta para nós.

    # Cria um processo para ordenar o lado esquerdo da lista
    pconn_left, cconn_left = Pipe()
    left_proc = Process(target=merge_sort_parallel, args=(lista[:ind], cconn_left, proc_num - 1))

    # Cria um processo para ordenar o lado direito da lista
    pconn_right, cconn_right = Pipe()
    right_proc = Process(target=merge_sort_parallel, args=(lista[ind:], cconn_right, proc_num - 1))

    # Inicia os dois subprocessos
    left_proc.start()
    right_proc.start()

    # Lembre-se de que a execução da expressão vai da primeira
    # avaliação dos argumentos de dentro para fora. Então aqui,
    # recebe as sublistas classificadas à esquerda e à direita
    # (cada uma recebe blocos, esperando para terminar), mescla
    # as duas sublistas ordenadas, então envia o resultado para
    # o processo pai através do argumento conn que recebemos.
    conn.send(merge(pconn_left.recv(), pconn_right.recv()))
    conn.close()

    # Junta os processos da esquerda e da direita.
    left_proc.join()
    right_proc.join()


def esta_ordenada(lista):
    """
    Retorna um booleano para indicar se a lista está ordernada
    ou não.
    """
    # Maneira elegante de retornar se a lista esta odernada ou
    # não.
    for i in range(1, len(lista)):
        if lista[i] < lista[i - 1]:
            return False
    return True


# Executa o método main agora que todas as dependências foram
# definidas.
# O if __name__ é para que o pydoc funcione e ainda possamos
# executar na linha de comando.
if __name__ == '__main__':
    main()
