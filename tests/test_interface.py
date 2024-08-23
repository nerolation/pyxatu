import re
from termcolor import colored
import pandas as pd
import logging
import unittest

import pyxatu

#logging.getLogger().setLevel(logging.CRITICAL)
#logging.disable(logging.CRITICAL)


xatu = pyxatu.PyXatu()


def dataframe_to_str(df):
    return re.sub(r'\s+', ' ', df.to_string()).strip()

def strip_str(s):
    return re.sub(r'\s+', ' ', s).strip()
    
def print_test_ok(test, how):
    print(
        "{:<70}".format(f"Test {test} ({how}) "),
        f"""{colored("OK","green"):>15}"""
    )
    
def print_test_failed(test, how):
    print(
        "{:<70}".format(f"Test {test} ({how}) "),
        f"""{colored("FAILED","red"):>15}"""
    )
    
def shorten_df(df):
    return pd.concat([df.iloc[0:10], df.iloc[-10:]], ignore_index=True)

class TestDataRetriever(unittest.TestCase):
    
    def test_configs_somewhere(self):
        one_good = False
        if xatu.read_clickhouse_config_from_env()[1] not in ["default_user", ""]:
            print_test_ok("read_clickhouse_config", "from_env")
            one_good = True

        if xatu.read_clickhouse_config_locally()[1] not in ["default_user", ""]:
            print_test_ok("read_clickhouse_config", "locally")
            one_good = True
            
        if not one_good:
            print_test_failed("read_clickhouse_config", "locally+env")

    def test_get_slots_exampleSlot(self):
        test = "xatu.get_slots"
        how = "exampleSlot"
        func = eval(test)
        exampleSlot = 9000000
        res = func(exampleSlot, columns="epoch, slot, meta_network_name, block_root, eth1_data_block_hash")
        expect = strip_str('    epoch     slot meta_network_name                                                          block_root                                                eth1_data_block_hash\n0  281250  9000000           mainnet  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)

    def test_get_slots_exampleSlotRange(self):
        test = "xatu.get_slots"
        how = "exampleSlotRange"
        func = eval(test)
        exampleSlotRange = [9000000, 9000010]
        res = func(exampleSlotRange, columns="epoch, slot, meta_network_name, block_root, eth1_data_block_hash", orderby="slot")
        expect = strip_str('    epoch     slot meta_network_name                                                          block_root                                                eth1_data_block_hash\n0  281250  9000000           mainnet  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n1  281250  9000001           mainnet  0x940b719767afe4fef9d191b4fdd43c948cafbdd62a6030b372bcf0f4f26aa780  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n2  281250  9000002           mainnet  0xf269e4f8bbb83141c35b0e571778641e2f52ced5d9b2150efe29324d80396cd1  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n3  281250  9000003           mainnet  0xad4469d969f3aef74b97d2702e4813ac67e042b2acb162303be1829c4bc4396e  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n4  281250  9000004           mainnet  0x6aac03dde90ac6a8291217f53702451913cab758a2df7aad48070aa36a3744b6  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n5  281250  9000005           mainnet  0x8c6828d5b7587549413bff72768f9aef70518bbf7a08d15b1e8d8e62e5e2bc60  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n6  281250  9000006           mainnet  0xc13e5aa29d6a31e3feb3194439c84ec664c42bf0ae82832435f503b4fa0030ce  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n7  281250  9000007           mainnet  0x5295094a722965bb5add25d1e46ec0a75d22267d279b64469e58a95cfed7ab5b  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n8  281250  9000008           mainnet  0x2c8920e3f1252f302512e276cbfebe992f845f53e487f17d3659fd21a5d5c5c8  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a\n9  281250  9000009           mainnet  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3  0x83c9761bbef2d2ee4297ccbc80bd440ec2d38b3c0c4c074f656002b78e8e909a')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)

    def test_get_slots_timeInterval(self):

        test = "xatu.get_slots"
        how = "timeInterval"
        func = eval(test)
        exampleSlotRange = [9314159, 9315159]
        time_interval = "365 days"
        res = func(slot=exampleSlotRange, time_interval=time_interval, columns="epoch, slot, meta_network_name, block_root, eth1_data_block_hash", orderby="slot")
        res = shorten_df(res)
        expect = strip_str('     epoch     slot meta_network_name                                                          block_root                                                eth1_data_block_hash\n0   291067  9314159           mainnet  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n1   291067  9314160           mainnet  0xdb06601771c2b335878c750a9688431b6bbf85160d3c4d169f316c3d6ebf04b8  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n2   291067  9314161           mainnet  0xd11d24ec3742d8e0c03423862a0de4865598e88e93d904543f46718c96a6293a  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n3   291067  9314162           mainnet  0xe3bb590f68d926797a0f7f0f0d12e874d32095d32025df49b913728a6e161d2b  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n4   291067  9314163           mainnet  0x739373c72281331ff7013a91e52185ae1217c88a0e386dcdc467023795a4dac9  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n5   291067  9314164           mainnet  0xcf900c95b83d73b6254d937b1b48e5fec9fc78fa63e8ad9337085b2a85c679cb  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n6   291067  9314165           mainnet  0x4e7f8361c4170d977a6470d2a5cba59050ad50d411c1e9fa2b5218991c976304  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n7   291067  9314166           mainnet  0x4213c58c19a6628696cdfe2dcb50e2557f1c4baed61f2fbfa11cce794a6bfa9b  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n8   291067  9314167           mainnet  0xfab5b2d8341298eec214135844ba2eb15bc948791310269870f73f6df906b880  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n9   291067  9314168           mainnet  0xbf39a370b3c6ccf4332292ea1cfd98dea20a0bab78671407b87333cc9ba4e95f  0x6f60cca43cbcbf4e751a5ac303f892b11ff875b96d496ce3d95fce24b25a7f0d\n10  291098  9315149           mainnet  0x08891ad9cebc2d3d1727daf364027f9515ad25229e3ba30da56e7887d586d385  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n11  291098  9315150           mainnet  0x1f2ce4cf44157962b875d469fd4b65e1440797a187ead3d71d4baab6cab3081f  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n12  291098  9315151           mainnet  0xfe8f959e8edc0a36357f9cfa52ffaaf00d4a2cc7cea268379a9fa301a23d0683  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n13  291098  9315152           mainnet  0x82692ef1e2216e1d58ea777c34a3b08dc42a249950212f74a88c423c451702cb  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n14  291098  9315153           mainnet  0xda51ada096a4f21b1afe38cfd085c16f2c657a8cc5d32eec1cce35707cfa6d7d  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n15  291098  9315154           mainnet  0x5387aca1b4a64b734e8643774d772ef0148fe349daaee42767e6eaf633e8372c  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n16  291098  9315155           mainnet  0x04c299e35b1de0223ec5e8c9c90d7f9f78fe90577dc13d9de9cd2b2fe483d0a9  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n17  291098  9315156           mainnet  0x006aa64f42024e740e44baa61230a90f4eb9d51df7922a23dfb53e510678295e  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n18  291098  9315157           mainnet  0x0d728214e15a24cc80b7de731ac9a0e8655d67c6da5a4edb74def34de5c41c65  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230\n19  291098  9315158           mainnet  0xb7df7a6a15e4a1cddaaf9b2ffbf9a1c539f419b007ff257c4671d9978a25ad4e  0x59950ea357101cfa54b35e26a03529e19a8eeb808f02afb342af4a17dc197230')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)


    def test_get_proposer_exampleSlot(self):

        test = "xatu.get_proposer"
        how = "exampleSlot"
        func = eval(test)
        exampleSlot = 9000000
        res = func(exampleSlot, columns="epoch, slot, meta_network_name, proposer_validator_index, proposer_pubkey")
        expect = strip_str('    epoch     slot meta_network_name  proposer_validator_index                                                                                     proposer_pubkey\n0  281250  9000000           mainnet                    912203  0x98fb8eacf684f80712faa9354535620f94a10687c2243c0cdae7280cf6220fb64c78e49efe8eef599406b33e5aac4dd0')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)


    def test_get_proposer_exampleSlotRange(self):
        test = "xatu.get_proposer"
        how = "exampleSlotRange"
        func = eval(test)
        exampleSlotRange = [9000000, 9000010]
        res = func(exampleSlotRange, columns="epoch, slot, meta_network_name, proposer_validator_index, proposer_pubkey", orderby="slot")
        expect = strip_str('    epoch     slot meta_network_name  proposer_validator_index                                                                                     proposer_pubkey\n0  281250  9000000           mainnet                    912203  0x98fb8eacf684f80712faa9354535620f94a10687c2243c0cdae7280cf6220fb64c78e49efe8eef599406b33e5aac4dd0\n1  281250  9000001           mainnet                    457928  0xb54f7dd4f2bafd83096a62d839ebc352451eb33e7d3838f20f745458f618e010591cc2a13bd535be4fdbea56fc14c35d\n2  281250  9000002           mainnet                   1306909  0xb98e51590aa5837e7ac06bc0f2e0db4133e5fab936228bfd1230f762c13e6d507b85b0dd194ce80bd4921f9358a06e3c\n3  281250  9000003           mainnet                    618690  0x88437aa9023f05cccaf1960e872b2ce8b8378a46938f9c33e5d9fde5cdf1de50158693f28da7844128810ca86b2abca1\n4  281250  9000004           mainnet                    561173  0x8ef5fc1c0b751270be59d3e5b82733950ec28be431dfe283292434d385854a03a898423b5f0bf4b8771861db0080b202\n5  281250  9000005           mainnet                    567215  0x8e0284d8f921a0b7414bb0849b03557c2dc38837b64cd3aa80ee191027790d0f7ff574a5d565369e60e0347cdbe693cd\n6  281250  9000006           mainnet                   1061578  0xb9d68c915d1afeb954f3a313307dc2a3fa2bd23ab62c2d7fc7818815da3121a15063f7c337b2aa9a6ba9ce11141cbcfe\n7  281250  9000007           mainnet                    802858  0xa225cb91474c070e4adf74b91a4f9e6f1e8fdb3cbaacc7653361d8904c7c9247428d453858ff943de421feab05abfe64\n8  281250  9000008           mainnet                   1357756  0x90d472bcfeec1c52f22c590b3c01f88f74e3a30fedca0a912ccabaeb2e3f485a83590fcf41da2a9b482d14af9d0d93c9\n9  281250  9000009           mainnet                   1087247  0x91fc45e6e1811cdb7a876bdec0bdf01770601c9d9295da0d1b759e62d1ca10a1bba42fb1dc61677ab5e53202caa3378d')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)


    def test_get_proposer_timeInterval(self):

        test = "xatu.get_proposer"
        how = "timeInterval"
        func = eval(test)
        exampleSlotRange = [9314159, 9315159]
        time_interval = "365 days"
        res = func(slot=exampleSlotRange, time_interval=time_interval, columns="epoch, slot, meta_network_name, proposer_validator_index, proposer_pubkey", orderby="slot")
        res = shorten_df(res)
        expect = strip_str('     epoch     slot meta_network_name  proposer_validator_index                                                                                     proposer_pubkey\n0   291067  9314159           mainnet                   1271120  0x8b1867cac5d099bffcb2129d136a1715df1018c00050cda02a58114baacc80e3a7bbe61c0309315d6c01a24db16a9ce3\n1   291067  9314160           mainnet                    346757  0x88a7e84b338396a5c4b24b5b3e298633d70dfb2e72403c144b071d634d21d01acae2e5572f0ccf0c15a934cf5b78bdb8\n2   291067  9314161           mainnet                   1170973  0x81c38e038c1d5f6fb82b6a9fc1ca4a1cc8ae29af82c971ab08161a3acb6f010b89b29bf06d3f4f9b4b62ec6ca4e0910e\n3   291067  9314162           mainnet                   1364520  0xa4ee82022830c6033ff7efe3470bbb9e455b97accde21945ec7b4106bc666af488166dbe6e5f87da61519aa53ccc5497\n4   291067  9314163           mainnet                    447216  0xa2d0edb8d95a2c8631df57ba23f62a832dd311a13428843989563fe97eba17c30a01b6faff1c6232ddaf3a13989b289c\n5   291067  9314164           mainnet                   1012691  0x84010e9de7808b55e8f999523fa71731311673dd6981264ac3f59ff21e449b40c2e0ccc9bb62a2858e14a5289e9346fa\n6   291067  9314165           mainnet                    923317  0xb0580167c8e5bd6aab82e6132c486b72b9d988e43730cff9348147287e141daf692bb21972fb6aafcc35671a2286b1f6\n7   291067  9314166           mainnet                    407043  0xb089e0312504287518844b04051c3cb9db9673da350e364bec41839ed4178e50066ca75aafdf23343e3e2054c412a1fc\n8   291067  9314167           mainnet                    474256  0xb9f5bc5330fdce4bf898f8a2044e1f77417701a58f23beb12080591cb80636322cb56b5f764db7a203b550202429393e\n9   291067  9314168           mainnet                    301348  0x8dd9c23dfc2af0d5c39c6a165d2dd05de8a73d9f36885fe6f664134796b19e3d02297cb6e6b4f8440cc6a70481e493d5\n10  291098  9315149           mainnet                    149755  0xa3925ed38fccf4af758991f0287a3875ddbdf2fba68c093dd1b07d4ad3220d3725f84a551b14687f3a11c1accdf19563\n11  291098  9315150           mainnet                      6141  0x96dadb6c11cbe67d9b18b6367e77f110b4f110aaf5c4a2c3b73e79c770f314629a7af5d659b5b86a97eddc112f0920c1\n12  291098  9315151           mainnet                    309680  0x924de09aca05313255cf7a9dc14d016f80dadf4d156e48f01c022c3daa98e443c4734f15f6e2a5ef89d5164976514f89\n13  291098  9315152           mainnet                    696193  0xb2f2a9bea6dfa985fef2a8d713fe82fa126707be432c92ee430b2eb3ba97ecaf68d28b3c4ddae3874db57aa454d672bf\n14  291098  9315153           mainnet                    810168  0xb21006e7abb397a29e962e8b74dd8cee3fabcacf2247fcc84d175ff43859e999c7ed32350c47fa4d060dbddb02937eec\n15  291098  9315154           mainnet                    755756  0x8bd5d91c88057eb8be47d014f2b129fb69bb17eeb3004fb50c184a582fc15b92ab10f27d68dc396a123fc5e8cd540159\n16  291098  9315155           mainnet                   1001515  0x930ace96ec029fae03ff0cbe1218159a998df4ce3c3d0e4bc687aa4725db57d1fbe7be079c1ad77573286afca2d29c84\n17  291098  9315156           mainnet                   1434407  0x8fd332aad426bd2821dcda55761e14115429f52b737f14185322aae1047157975db0f68fe8d4b688cfceadb10f921b0f\n18  291098  9315157           mainnet                   1361028  0x9769d6b605cdd1d3a65bca88b2a01e353b448a39c55e37840dcd9de1f4564b5a1dbab7f06cac72e69a98cb7576035571\n19  291098  9315158           mainnet                   1313033  0x867b0cf3210d53ae3a5111ef73e43f6aba617b74119f29e015e2568fdd8828feeff56fe1cea9468764ce3c4dfc66c771')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)

    def test_get_blockevent_exampleSlot(self):

        test = "xatu.get_blockevent"
        how = "exampleSlot"
        func = eval(test)
        exampleSlot = 9000000
        res = func(exampleSlot, columns="epoch, slot, meta_network_name, event_date_time", orderby="event_date_time")
        res = shorten_df(res)
        expect = strip_str('     epoch     slot meta_network_name          event_date_time\n0   281250  9000000           mainnet  2024-05-04 12:00:26.263\n1   281250  9000000           mainnet  2024-05-04 12:00:26.279\n2   281250  9000000           mainnet  2024-05-04 12:00:27.208\n3   281250  9000000           mainnet  2024-05-04 12:00:27.359\n4   281250  9000000           mainnet  2024-05-04 12:00:27.637\n5   281250  9000000           mainnet  2024-05-04 12:00:28.076\n6   281250  9000000           mainnet  2024-05-04 12:00:28.214\n7   281250  9000000           mainnet  2024-05-04 12:00:28.517\n8   281250  9000000           mainnet  2024-05-04 12:00:29.080\n9   281250  9000000           mainnet  2024-05-04 12:00:29.496\n10  281250  9000000           mainnet  2024-05-04 12:00:27.637\n11  281250  9000000           mainnet  2024-05-04 12:00:28.076\n12  281250  9000000           mainnet  2024-05-04 12:00:28.214\n13  281250  9000000           mainnet  2024-05-04 12:00:28.517\n14  281250  9000000           mainnet  2024-05-04 12:00:29.080\n15  281250  9000000           mainnet  2024-05-04 12:00:29.496\n16  281250  9000000           mainnet  2024-05-04 12:00:29.681\n17  281250  9000000           mainnet  2024-05-04 12:00:30.507\n18  281250  9000000           mainnet  2024-05-04 12:00:38.710\n19  281250  9000000           mainnet  2024-05-04 12:00:40.885')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)

    def test_get_blockevent_exampleSlotRange(self):

        test = "xatu.get_blockevent"
        how = "exampleSlotRange"
        func = eval(test)
        exampleSlotRange = [9000000, 9000010]
        res = func(exampleSlotRange, columns="epoch, slot, meta_network_name, event_date_time", orderby="slot, event_date_time")
        res = shorten_df(res)
        expect = strip_str('     epoch     slot meta_network_name          event_date_time\n0   281250  9000000           mainnet  2024-05-04 12:00:26.263\n1   281250  9000000           mainnet  2024-05-04 12:00:26.279\n2   281250  9000000           mainnet  2024-05-04 12:00:27.208\n3   281250  9000000           mainnet  2024-05-04 12:00:27.359\n4   281250  9000000           mainnet  2024-05-04 12:00:27.637\n5   281250  9000000           mainnet  2024-05-04 12:00:28.076\n6   281250  9000000           mainnet  2024-05-04 12:00:28.214\n7   281250  9000000           mainnet  2024-05-04 12:00:28.517\n8   281250  9000000           mainnet  2024-05-04 12:00:29.080\n9   281250  9000000           mainnet  2024-05-04 12:00:29.496\n10  281250  9000009           mainnet  2024-05-04 12:02:13.627\n11  281250  9000009           mainnet  2024-05-04 12:02:13.656\n12  281250  9000009           mainnet  2024-05-04 12:02:13.669\n13  281250  9000009           mainnet  2024-05-04 12:02:13.772\n14  281250  9000009           mainnet  2024-05-04 12:02:13.937\n15  281250  9000009           mainnet  2024-05-04 12:02:14.016\n16  281250  9000009           mainnet  2024-05-04 12:02:14.122\n17  281250  9000009           mainnet  2024-05-04 12:02:14.131\n18  281250  9000009           mainnet  2024-05-04 12:02:14.327\n19  281250  9000009           mainnet  2024-05-04 12:02:14.523')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)

    def test_get_blockevent_timeInterval(self):

        test = "xatu.get_blockevent"
        how = "timeInterval"
        func = eval(test)
        exampleSlotRange = [9314159, 9315159]
        time_interval = "365 days"
        res = func(slot=exampleSlotRange, time_interval=time_interval, columns="epoch, slot, meta_network_name, event_date_time", orderby="slot, event_date_time")
        res = shorten_df(res)
        expect = strip_str('     epoch     slot meta_network_name          event_date_time\n0   291067  9314159           mainnet  2024-06-17 03:12:14.596\n1   291067  9314159           mainnet  2024-06-17 03:12:14.667\n2   291067  9314159           mainnet  2024-06-17 03:12:14.718\n3   291067  9314159           mainnet  2024-06-17 03:12:14.820\n4   291067  9314159           mainnet  2024-06-17 03:12:14.830\n5   291067  9314159           mainnet  2024-06-17 03:12:14.895\n6   291067  9314159           mainnet  2024-06-17 03:12:14.948\n7   291067  9314159           mainnet  2024-06-17 03:12:15.120\n8   291067  9314159           mainnet  2024-06-17 03:12:15.143\n9   291067  9314159           mainnet  2024-06-17 03:12:15.222\n10  291098  9315158           mainnet  2024-06-17 06:32:03.186\n11  291098  9315158           mainnet  2024-06-17 06:32:03.192\n12  291098  9315158           mainnet  2024-06-17 06:32:03.251\n13  291098  9315158           mainnet  2024-06-17 06:32:03.265\n14  291098  9315158           mainnet  2024-06-17 06:32:03.315\n15  291098  9315158           mainnet  2024-06-17 06:32:03.493\n16  291098  9315158           mainnet  2024-06-17 06:32:03.593\n17  291098  9315158           mainnet  2024-06-17 06:32:03.720\n18  291098  9315158           mainnet  2024-06-17 06:32:04.470\n19  291098  9315158           mainnet  2024-06-17 06:32:04.950')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)

    def test_get_attestation_exampleSlot(self):

        test = "xatu.get_attestation"
        how = "exampleSlot"
        func = eval(test)
        exampleSlot = 9000000
        res = func(exampleSlot, columns="slot, block_slot, validators, beacon_block_root", orderby="slot, block_slot, beacon_block_root, validators", limit=10)
        res = shorten_df(res)
        expect = strip_str('       slot  block_slot validators                                                   beacon_block_root\n0   9000000     9000001      27036  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n1   9000000     9000001     818774  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n2   9000000     9000001     525990  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n3   9000000     9000001    1064538  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n4   9000000     9000001    1165372  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n5   9000000     9000001     231665  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n6   9000000     9000001    1295496  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n7   9000000     9000001      65068  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n8   9000000     9000001     929102  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n9   9000000     9000001     282532  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n10  9000000     9000001     220770  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n11  9000000     9000001     216611  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n12  9000000     9000001     150010  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n13  9000000     9000001    1000237  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n14  9000000     9000001     943001  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n15  9000000     9000001    1309695  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n16  9000000     9000001     335973  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n17  9000000     9000001     378512  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n18  9000000     9000001     930843  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n19  9000000     9000001    1178931  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)

    def test_get_attestation_exampleSlotRange(self):

        test = "xatu.get_attestation"
        how = "exampleSlotRange"
        func = eval(test)
        exampleSlotRange = [9000000, 9000001]
        res = func(exampleSlotRange, columns="slot, block_slot, validators, beacon_block_root", orderby="slot, block_slot, beacon_block_root, validators", limit=10)
        res = shorten_df(res)
        expect = strip_str('       slot  block_slot validators                                                   beacon_block_root\n0   9000000     9000001      27036  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n1   9000000     9000001     818774  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n2   9000000     9000001     525990  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n3   9000000     9000001    1064538  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n4   9000000     9000001    1165372  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n5   9000000     9000001     231665  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n6   9000000     9000001    1295496  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n7   9000000     9000001      65068  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n8   9000000     9000001     929102  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n9   9000000     9000001     282532  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n10  9000000     9000001     220770  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n11  9000000     9000001     216611  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n12  9000000     9000001     150010  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n13  9000000     9000001    1000237  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n14  9000000     9000001     943001  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n15  9000000     9000001    1309695  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n16  9000000     9000001     335973  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n17  9000000     9000001     378512  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n18  9000000     9000001     930843  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n19  9000000     9000001    1178931  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)

    def test_get_attestation_timeInterval(self):

        test = "xatu.get_attestation"
        how = "timeInterval"
        func = eval(test)
        exampleSlotRange = 9000000
        time_interval = "365 days"
        res = func(slot=exampleSlotRange, time_interval=time_interval, columns="slot, block_slot, validators, beacon_block_root", orderby="slot, block_slot, beacon_block_root, validators", limit=10)
        res = shorten_df(res)
        expect = strip_str('       slot  block_slot validators                                                   beacon_block_root\n0   9000000     9000001      27036  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n1   9000000     9000001     818774  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n2   9000000     9000001     525990  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n3   9000000     9000001    1064538  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n4   9000000     9000001    1165372  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n5   9000000     9000001     231665  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n6   9000000     9000001    1295496  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n7   9000000     9000001      65068  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n8   9000000     9000001     929102  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n9   9000000     9000001     282532  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n10  9000000     9000001     220770  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n11  9000000     9000001     216611  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n12  9000000     9000001     150010  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n13  9000000     9000001    1000237  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n14  9000000     9000001     943001  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n15  9000000     9000001    1309695  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n16  9000000     9000001     335973  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n17  9000000     9000001     378512  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n18  9000000     9000001     930843  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n19  9000000     9000001    1178931  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6')
        actual = dataframe_to_str(res)
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)
            
            
    def test_get_attestation_event_exampleSlot(self):

        test = "xatu.get_attestation_event"
        how = "exampleSlot"
        func = eval(test)
        exampleSlot = 9000000
        res = func(exampleSlot, columns="epoch, slot, meta_network_name, attesting_validator_index, beacon_block_root", orderby="slot, beacon_block_root")
        res = shorten_df(res)
        expect = strip_str('     epoch     slot  block_slot meta_network_name validators                                                   beacon_block_root\n0   281250  9000000     9000001           mainnet      27036  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n1   281250  9000000     9000001           mainnet     818774  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n2   281250  9000000     9000001           mainnet     525990  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n3   281250  9000000     9000001           mainnet    1064538  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n4   281250  9000000     9000001           mainnet    1165372  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n5   281250  9000000     9000001           mainnet     231665  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n6   281250  9000000     9000001           mainnet    1295496  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n7   281250  9000000     9000001           mainnet      65068  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n8   281250  9000000     9000001           mainnet     929102  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n9   281250  9000000     9000001           mainnet     282532  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n10  281250  9000000     9000030           mainnet     770136  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n11  281250  9000000     9000030           mainnet     410342  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n12  281250  9000000     9000030           mainnet    1329275  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n13  281250  9000000     9000030           mainnet    1041064  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n14  281250  9000000     9000030           mainnet     903579  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n15  281250  9000000     9000030           mainnet    1242212  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n16  281250  9000000     9000030           mainnet    1220573  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n17  281250  9000000     9000030           mainnet    1026592  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n18  281250  9000000     9000030           mainnet     185881  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n19  281250  9000000     9000030           mainnet     428850  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6')
        actual = dataframe_to_str(res)
        #if expect == actual:
        #    print_test_ok(test, how)
        #else:
        #    print_test_failed(test, how)

    def test_get_attestation_event_exampleSlotRange(self):

        test = "xatu.get_attestation_event"
        how = "exampleSlotRange"
        func = eval(test)
        exampleSlotRange = [9000000, 9000010]
        res = func(exampleSlotRange, columns="epoch, slot, meta_network_name, attesting_validator_index, beacon_block_root", orderby="slot, beacon_block_root")
        res = shorten_df(res)
        expect = strip_str('     epoch     slot  block_slot meta_network_name validators                                                   beacon_block_root\n0   281250  9000000     9000001           mainnet      27036  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n1   281250  9000000     9000001           mainnet     818774  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n2   281250  9000000     9000001           mainnet     525990  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n3   281250  9000000     9000001           mainnet    1064538  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n4   281250  9000000     9000001           mainnet    1165372  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n5   281250  9000000     9000001           mainnet     231665  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n6   281250  9000000     9000001           mainnet    1295496  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n7   281250  9000000     9000001           mainnet      65068  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n8   281250  9000000     9000001           mainnet     929102  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n9   281250  9000000     9000001           mainnet     282532  0xcc8a36da0d5112c8dd602530ac7c7b8364edfd92cdc6f0d62365de392e8e5bb6\n10  281250  9000009     9000054           mainnet     484179  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n11  281250  9000009     9000054           mainnet     961742  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n12  281250  9000009     9000054           mainnet      22687  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n13  281250  9000009     9000054           mainnet     379177  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n14  281250  9000009     9000054           mainnet     298751  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n15  281250  9000009     9000054           mainnet     514324  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n16  281250  9000009     9000054           mainnet    1134305  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n17  281250  9000009     9000054           mainnet     194459  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n18  281250  9000009     9000054           mainnet     387907  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3\n19  281250  9000009     9000054           mainnet    1056242  0x868b1248362f21d40fd8d7fa844178ba3bb0c243113c6a24ad9e588410f3f1d3')
        actual = dataframe_to_str(res)
        #if expect == actual:
        #    print_test_ok(test, how)
        #else:
        #    print_test_failed(test, how)

    def test_get_attestation_event_timeInterval(self):

        test = "xatu.get_attestation_event"
        how = "timeInterval"
        func = eval(test)
        exampleSlotRange = [9314159, 9314160]
        time_interval = "365 days"
        res = func(slot=exampleSlotRange, time_interval=time_interval, columns="epoch, slot, meta_network_name, attesting_validator_index, beacon_block_root", orderby="slot, beacon_block_root", limit=10)
        res = shorten_df(res)
        expect = strip_str('     epoch     slot  block_slot meta_network_name validators                                                   beacon_block_root\n0   291067  9314159     9314160           mainnet      19497  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n1   291067  9314159     9314160           mainnet    1244571  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n2   291067  9314159     9314160           mainnet     865594  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n3   291067  9314159     9314160           mainnet     327510  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n4   291067  9314159     9314160           mainnet    1135475  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n5   291067  9314159     9314160           mainnet     264110  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n6   291067  9314159     9314160           mainnet     704041  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n7   291067  9314159     9314160           mainnet     436728  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n8   291067  9314159     9314160           mainnet     403003  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n9   291067  9314159     9314160           mainnet     330022  0x9e29dd208381bb7c561d651079573481add5c8fa77a2306e9b2f4e1de99bc9d3\n10  291067  9314159     9314171           mainnet     860220  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n11  291067  9314159     9314171           mainnet     982340  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n12  291067  9314159     9314171           mainnet    1078042  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n13  291067  9314159     9314171           mainnet    1013415  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n14  291067  9314159     9314171           mainnet     703912  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n15  291067  9314159     9314171           mainnet    1308220  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n16  291067  9314159     9314171           mainnet    1237657  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n17  291067  9314159     9314171           mainnet     827864  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n18  291067  9314159     9314171           mainnet    1237985  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383\n19  291067  9314159     9314171           mainnet    1416180  0xfd4edfa59c12d801496debaff49de3b70e93f0f69e261d9ba17fdf9b2d1ca383')
        actual = dataframe_to_str(res)
        #if expect == actual:
        #    print_test_ok(test, how)
        #else:
        #    print_test_failed(test, how)

    def test_get_reorgs_exampleSlotRange(self):

        test = "xatu.get_reorgs"
        how = "exampleSlotRange"
        func = eval(test)
        exampleSlotRange = [9000000, 9005100]
        res = func(exampleSlotRange, orderby="slot")
        expect = '      slot\n0  9001619\n1  9002322\n2  9002396\n3  9002713\n4  9002896\n5  9003104\n6  9004001\n7  9004066\n8  9004675\n9  9004856'
        actual = res.to_string()
        if expect == actual:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)


    def test_get_reorgs_timeInterval(self):

        test = "xatu.get_reorgs"
        how = "timeInterval"
        func = eval(test)
        time_interval = "30 days"
        res = func(time_interval=time_interval)
        if len(res) > 0:
            print_test_ok(test, how)
        else:
            print_test_failed(test, how)
            
    def test_get_missed_slots_exampleSlotRange(self):

        test = "xatu.get_missed_slots"
        how = "exampleSlotRange"
        func = eval(test)
        exampleSlotRange = [9000000, 9005100]
        res = list(func(exampleSlotRange, columns="slot", orderby="slot"))
        res = res[0:10] + res[-10:]
        expect = '[9000961, 9001089, 9001985, 9004675, 9000840, 9002896, 9001619, 9000726, 9004568, 9004696, 9002713, 9000921, 9003104, 9004001, 9001058, 9000803, 9004258, 9004897, 9002486, 9005047]'
        actual = str(res)
        self.assertEqual(expect, actual)
        print_test_ok(test, how)
    


    def test_get_missed_slots_timeInterval(self):

        test = "xatu.get_missed_slots"
        how = "timeInterval"
        func = eval(test)
        time_interval = "30 days"
        res = list(func(time_interval=time_interval, columns="slot",))
        res = res[0:10] + res[-10:]
        self.assertGreater(len(res), 0)
        print_test_ok(test, how)
        
        
    def test_get_elaborated_attestations_exampleSlotRange(self):
        test = "xatu.get_elaborated_attestations"
        how = "exampleSlotRange"
        func = eval(test)
        exampleEpochRange = [9000000, 9000001]
        res = func(slot = exampleEpochRange, columns="slot, block_slot, validators, beacon_block_root", orderby="slot, block_slot, beacon_block_root, validators", limit=10)
        df = shorten_df(res)
        expect = strip_str('       slot  validator   status     vote_type\n0   9000000     761856  offline        source\n1   9000000     901120  offline        source\n2   9000000     647172  offline        source\n3   9000000     860165  offline        source\n4   9000000     163849  offline        source\n5   9000000     155662  offline        source\n6   9000000    1081358  offline        source\n7   9000000         17  offline        source\n8   9000000    1368082  offline        source\n9   9000000     860179  offline        source\n10  9000000     925674  offline  beacon_block\n11  9000000     884715  offline  beacon_block\n12  9000000    1163243  offline  beacon_block\n13  9000000    1212393  offline  beacon_block\n14  9000000     909296  offline  beacon_block\n15  9000000     770034  offline  beacon_block\n16  9000000     819186  offline  beacon_block\n17  9000000     851954  offline  beacon_block\n18  9000000    1056755  offline  beacon_block\n19  9000000     925690  offline  beacon_block')
        actual = dataframe_to_str(df)
        self.assertEqual(expect, actual)
        print_test_ok(test, how)
            
            
    def test_get_mevboost_get_payloads_exampleSlot(self):
        test = "xatu.mevboost.get_payloads"
        how = "exampleSlot"
        func = eval(test)
        exampleSlot = 9755263
        df = func(slot = exampleSlot, limit=1)
        expect = strip_str('                    relay     slot                                                          block_hash                                                                                      builder_pubkey                                                                                     proposer_pubkey                      proposer_fee_recipient         value  gas_used  gas_limit  block_number  num_tx\n0                  aestus  9755263  0x6d3bf98d453615c76599c6906ef4028219ba059efafc7656914d3c14212539a6  0x8194927433533129c9d7a5863fe39f76c008d76d52c8e3636d358bebcb2f3a72b893b93a5ed2ccbecd9182307fe180d7  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  3.844469e+20   3177944   30000000      20547472      23\n1         agnostic gnosis  9755260  0xb117d09c305f66c570bb5fc32166d5a5c9748b85aa26f26cc3720e906954153d  0xa9d0a0f9059972d775a45d8377768ff20234de91a6fbacba5737ff8803807c38021b5863e5084869e695ef1d6c2fdaef  0xae7a1cb54ca7f279edaa5b79ebc3a76cc9520c350abe56ac989de0ea391c4905c901088afc25847127651aa85d754a65  0x388C818CA8B9251b393131C08a736A67ccB19297  4.626064e+16  19068894   30000000      20547469     156\n2  bloxroute (max profit)  9755261  0xbec6c8fcec2f26af3c895c67b11828ba55a5632994cee2c6b1dc1c4b8f7da163  0x978a35c39c41aadbe35ea29712bccffb117cc6ebcad4d86ea463d712af1dc80131d0c650dc29ba29ef27c881f43bd587  0xa713fa0c4537c05baf0a92de4d8baedffe1c1f8cda6466ae9e711f6ff41d27ed9a595be2d48852aad8534de8e5653658  0x4675C7e5BaAFBFFbca748158bEcBA61ef3b0a263  1.467345e+16   7061264   30000000      20547470      90\n3   bloxroute (regulated)  9755260  0xb117d09c305f66c570bb5fc32166d5a5c9748b85aa26f26cc3720e906954153d  0xa9d0a0f9059972d775a45d8377768ff20234de91a6fbacba5737ff8803807c38021b5863e5084869e695ef1d6c2fdaef  0xae7a1cb54ca7f279edaa5b79ebc3a76cc9520c350abe56ac989de0ea391c4905c901088afc25847127651aa85d754a65  0x388C818CA8B9251b393131C08a736A67ccB19297  4.626064e+16  19068894   30000000      20547469     156\n4                    eden  9751498  0x2503e4ed7efde8cfe4d1ca2debc736bd3ad2ecc7624a2577d3444af8a64e27df  0x8e39849ceabc8710de49b2ca7053813de18b1c12d9ee22149dac4b90b634dd7e6d1e7d3c2b4df806ce32c6228eb70a8b  0xa643e63079cbcd4e323c0f203f5f654f44fe73c9dabac72d3c6d048b6879fa069ed92490c3d9118af9afc03100798bc5  0x3EC0977544889c0Bb3115e5e8C654E24CD36E7D3  5.027104e+15   6319406   30000000      20543736     103\n5               flashbots  9755261  0xbec6c8fcec2f26af3c895c67b11828ba55a5632994cee2c6b1dc1c4b8f7da163  0x978a35c39c41aadbe35ea29712bccffb117cc6ebcad4d86ea463d712af1dc80131d0c650dc29ba29ef27c881f43bd587  0xa713fa0c4537c05baf0a92de4d8baedffe1c1f8cda6466ae9e711f6ff41d27ed9a595be2d48852aad8534de8e5653658  0x4675C7e5BaAFBFFbca748158bEcBA61ef3b0a263  1.467345e+16   7061264   30000000      20547470      90\n6                manifold  9754975  0x4c47054fd1952a90a750224e27a396f1e005a40810cadefb905a440d9b6b1166  0xb783f81b2929e70c5b9a066d082d85dd12efa1c3e95185e18aabe6f4f93e387618532dc63ff80fdc3d9704f0ca5bc8c3  0x86c720bdf85dd8fa944ae434cbe987de6e33df7e106cb28523e9d7abd7dfaf454e6a9cac4ffa3768ce54bfd2f9acdfe2  0x829400C84E2dF1fBD6012e2Fb3EA8Bb0866D2651  1.048696e+17   6701465   30000000      20547186      83\n7                   titan  9755263  0x6d3bf98d453615c76599c6906ef4028219ba059efafc7656914d3c14212539a6  0x8194927433533129c9d7a5863fe39f76c008d76d52c8e3636d358bebcb2f3a72b893b93a5ed2ccbecd9182307fe180d7  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3d9cf8e163bbc840195a97e81f8a34e295b8f39  3.844469e+20   3177944   30000000      20547472      23\n8             ultra sound  9755260  0xb117d09c305f66c570bb5fc32166d5a5c9748b85aa26f26cc3720e906954153d  0xa9d0a0f9059972d775a45d8377768ff20234de91a6fbacba5737ff8803807c38021b5863e5084869e695ef1d6c2fdaef  0xae7a1cb54ca7f279edaa5b79ebc3a76cc9520c350abe56ac989de0ea391c4905c901088afc25847127651aa85d754a65  0x388C818CA8B9251b393131C08a736A67ccB19297  4.626064e+16  19068894   30000000      20547469     156')
        actual = dataframe_to_str(df)
        self.assertEqual(expect, actual)
        print_test_ok(test, how)
        
               
    def test_get_mevboost_get_bids_exampleSlot(self):
        test = "xatu.mevboost.get_bids"
        how = "exampleSlotRange"
        func = eval(test)
        exampleSlot = 9755263
        df = func(slot = exampleSlot)
        df = df.iloc[0:10]
        expect = strip_str('    relay   timestamp   timestamp_ms     slot                                                          block_hash                                                                                      builder_pubkey                                                                                     proposer_pubkey                      proposer_fee_recipient         value  gas_used  gas_limit  block_number  num_tx  optimistic_submission\n0  aestus  1723887178  1723887178701  9755263  0xd8a22dbb5d5b5c155bf7bf60ca6be02111f12c26d10c0eed5ad5548039bd0312  0x8aab0ed724d2c7f94af139bd2249ab511f08474ac69e761e56918403c81c358f5f8a6d61c62a86dc4cd7bcad935f49d9  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.916456e+16  16441190   30000000      20547472     142                      0\n1  aestus  1723887178  1723887178661  9755263  0xfe375d73db859e31a7ce7a578236d4ef0d2a76cdb92db71a1581695821a766d9  0xacfdcf458829f4693168a57d0659253069d687682bc64ec130d935ecb6e05ccfb80c138bd3cf53546c86715696612ec8  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.916535e+16  16365889   30000000      20547472     142                      0\n2  aestus  1723887178  1723887178644  9755263  0x856e041921b4e2bf90149b4874532ad42612d7988c5e937e9f153f4df70ed30c  0x8e6df6e0a9ca3fd89db2aa2f3daf77722dc4fbcd15e285ed7d9560fdf07b7d69ba504add4cc12ac999b8094ff30ed06c  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.916535e+16  16365889   30000000      20547472     142                      0\n3  aestus  1723887178  1723887178634  9755263  0x92ce58f72ba638ccd4f5d9dfa2c79891c8a7de323c3bf641dd81756b1f31f145  0x8aab0ed724d2c7f94af139bd2249ab511f08474ac69e761e56918403c81c358f5f8a6d61c62a86dc4cd7bcad935f49d9  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.915168e+16  16229211   30000000      20547472     141                      0\n4  aestus  1723887178  1723887178527  9755263  0x10ffad1481b320a2f1c4d8b18a20c8de92e98a1225e0b32bf37973273fea9792  0xacfdcf458829f4693168a57d0659253069d687682bc64ec130d935ecb6e05ccfb80c138bd3cf53546c86715696612ec8  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.912486e+16  16241159   30000000      20547472     141                      0\n5  aestus  1723887178  1723887178513  9755263  0x065bd7bb101f4fd1323a93f1419fbfb0a0f797643277d8974da633b0d811b48f  0xacfdcf458829f4693168a57d0659253069d687682bc64ec130d935ecb6e05ccfb80c138bd3cf53546c86715696612ec8  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.850558e+16  16111229   30000000      20547472     140                      0\n6  aestus  1723887178  1723887178515  9755263  0x64882d64e8d88db63fcdc11bd18b11f84b2870a1ea36b6d37c82f3d3bfd81d17  0x8e6df6e0a9ca3fd89db2aa2f3daf77722dc4fbcd15e285ed7d9560fdf07b7d69ba504add4cc12ac999b8094ff30ed06c  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.875981e+16  16236618   30000000      20547472     141                      0\n7  aestus  1723887178  1723887178554  9755263  0x74b3e6b21cff74118109689ace7ec04e96802fe8a980016b1d73fc23023baa34  0x8e6df6e0a9ca3fd89db2aa2f3daf77722dc4fbcd15e285ed7d9560fdf07b7d69ba504add4cc12ac999b8094ff30ed06c  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.912486e+16  16241159   30000000      20547472     141                      0\n8  aestus  1723887178  1723887178515  9755263  0xde9244525a26c11f90b9a8d7a2d78fd128f6df8dde02fc88f59533bda36e5f7d  0x8aab0ed724d2c7f94af139bd2249ab511f08474ac69e761e56918403c81c358f5f8a6d61c62a86dc4cd7bcad935f49d9  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.874614e+16  16099940   30000000      20547472     140                      0\n9  aestus  1723887178  1723887178713  9755263  0xe4a913cf824a84dbd5d5385fcdef11b563f6807fb9f2b9ea6643488936e5b701  0x8e6df6e0a9ca3fd89db2aa2f3daf77722dc4fbcd15e285ed7d9560fdf07b7d69ba504add4cc12ac999b8094ff30ed06c  0xb0336d0bc91eb72a55808f68c059a5a8b3bdfe4557abfe6565da798e13033eda7ec023a7773a739fbefdbddce63fcf4b  0xb3D9cf8E163bbc840195a97E81F8A34E295B8f39  1.917823e+16  16577868   30000000      20547472     143                      0')
        actual = dataframe_to_str(df)
        self.assertEqual(expect, actual)
        print_test_ok(test, how)
            
if __name__ == '__main__':
    unittest.main()