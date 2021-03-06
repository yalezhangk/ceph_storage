PTYPE = {
    'regular': {
        'journal': {
            # identical because creating a journal is atomic
            'ready': '45b0969e-9b03-4f30-b4c6-b4b80ceff217',
            'tobe': '45b0969e-9b03-4f30-b4c6-b4b80ceff217',
        },
        'block': {
            # identical because creating a block is atomic
            'ready': 'cafecafe-9b03-4f30-b4c6-b4b80ceff217',
            'tobe': 'cafecafe-9b03-4f30-b4c6-b4b80ceff217',
        },
        'block.db': {
            # identical because creating a block is atomic
            'ready': '30cd0809-c2b2-499c-8879-2d6b78529987',
            'tobe': '30cd0809-c2b2-499c-8879-2d6b785293cf',
        },
        'block.wal': {
            # identical because creating a block is atomic
            'ready': '5ce17fce-4087-4169-b7ff-056cc584740a',
            'tobe': '5ce17fce-4087-4169-b7ff-056cc58473cf',
        },
        'block.t2ce': {
            'ready': '9edda69e-d60c-4db5-ab3b-5f2f9a8e8473',
            'tobe': '3993b747-8ad6-489f-b5ef-9d74844bd25d',
        },
        'osd': {
            'ready': '4fbd7e29-9d25-41b8-afd0-062c0ceff16e',
            'tobe': '89c57f98-2fe5-4dc0-89c1-f3ad0ceff3cf',
        },
        'lockbox': {
            'ready': 'fb3aabf9-d25f-47cc-bf5e-721d18164a7c',
            'tobe': 'fb3aabf9-d25f-47cc-bf5e-721d181643cf',
        },
    },
    'luks': {
        'journal': {
            'ready': '45b0969e-9b03-4f30-b4c6-35865ceff217',
            'tobe': '89c57f98-2fe5-4dc0-89c1-35865ceff3cf',
        },
        'block': {
            'ready': 'cafecafe-9b03-4f30-b4c6-35865ceff217',
            'tobe': '89c57f98-2fe5-4dc0-89c1-35865ceff3cf',
        },
        'block.db': {
            'ready': '166418da-c469-4022-adf4-b30afd37f287',
            'tobe': '7521c784-4626-4260-bc8d-ba77a0f5f3cf',
        },
        'block.wal': {
            'ready': '86a32090-3647-40b9-bbbd-38d8c573ab97',
            'tobe': '92dad30f-175b-4d40-a5b0-5c0a258b43cf',
        },
        'block.t2ce': {
            'ready': 'e515bc64-247b-4b31-b340-3ea49d7ac13d',
            'tobe': '67ee8e88-d2ba-4e84-b7b0-b08850f42955',
        },
        'osd': {
            'ready': '4fbd7e29-9d25-41b8-afd0-35865ceff16e',
            'tobe': '89c57f98-2fe5-4dc0-89c1-5ec00ceff3cf',
        },
    },
    'plain': {
        'journal': {
            'ready': '45b0969e-9b03-4f30-b4c6-5ec00ceff217',
            'tobe': '89c57f98-2fe5-4dc0-89c1-35865ceff3cf',
        },
        'block': {
            'ready': 'cafecafe-9b03-4f30-b4c6-5ec00ceff217',
            'tobe': '89c57f98-2fe5-4dc0-89c1-35865ceff3cf',
        },
        'block.db': {
            'ready': '93b0052d-02d9-4d8a-a43b-33a3ee4dfcd4',
            'tobe': '69d17c68-3e58-4399-aff0-b68265f2e3cf',
        },
        'block.wal': {
            'ready': '306e8683-4fe2-4330-b7c0-00a917c16a77',
            'tobe': 'f2d89683-a621-4063-964a-eb1f7863a3cf',
        },
        'block.t2ce': {
            'ready': '951b9860-9cd7-472c-bb76-3acc76f04663',
            'tobe': '90df66c9-9508-4740-bc35-bf564695c0b6',
        },
        'osd': {
            'ready': '4fbd7e29-9d25-41b8-afd0-5ec00ceff16e',
            'tobe': '89c57f98-2fe5-4dc0-89c1-5ec00ceff3cf',
        },
    },
    'mpath': {
        'journal': {
            'ready': '45b0969e-8ae0-4982-bf9d-5a8d867af671',
            'tobe': '45b0969e-8ae0-4982-bf9d-5a8d867af671',
        },
        'block': {
            'ready': 'cafecafe-8ae0-4982-bf9d-5a8d867af671',
            'tobe': 'cafecafe-8ae0-4982-bf9d-5a8d867af671',
        },
        'block.db': {
            'ready': 'ec6d6385-e346-45dc-be91-da2a7c8b3372',
            'tobe': 'ec6d6385-e346-45dc-be91-da2a7c8b33cf',
        },
        'block.wal': {
            'ready': '01b41e1b-002a-453c-9f17-88793989f090',
            'tobe': '01b41e1b-002a-453c-9f17-88793989f3cf',
        },
        'block.t2ce': {
            'ready': '2d5fb39f-bfcb-4f5e-8620-cf00b783c93f',
            'tobe': 'e6a7d498-0d14-4ad3-92d9-db583ce46a27',
        },
        'osd': {
            'ready': '4fbd7e29-8ae0-4982-bf9d-5a8d867af671',
            'tobe': '89c57f98-8ae0-4982-bf9d-5a8d867af671',
        },
        'lockbox': {
            'ready': '7f4a666a-16f3-47a2-8445-152ef4d0307d',
            'tobe': '7f4a666a-16f3-47a2-8445-152ef4d033cf',
        },
    },
}
