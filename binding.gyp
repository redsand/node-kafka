{
  "targets": [
    {
      'target_name': 'librdkafkaBinding',
      'sources': [
        'src/kafka.cpp',
      ],
      'include_dirs': [
        'deps/librdkafka',
        'deps/librdkafka/src',
        'deps/librdkafka/src-cpp'
      ],
      'dependencies': [
        'deps/librdkafka/librdkafka.gyp:librdkafka',
        'deps/librdkafka/librdkafka.gyp:librdkafka++',
      ],
      'cppflags_cc': [
	'-fexceptions',
	'-MD',
	'-MP',
	'-fPIC',
        '-Wall',
        '-O3'
      ],
      'cppflags': [
	'-fexceptions',
	'-MD',
	'-MP',
	'-fPIC',
        '-Wall',
        '-O3'
      ],
      'cflags!': ['-fno-exceptions','-fno-rtti'],
      'cflags_cc!': ['-fno-exceptions','-fno-rtti'],
      'conditions': [
        [
          'OS=="mac"',
          {
            'xcode_settings': {
              'MACOSX_DEPLOYMENT_TARGET': '10.7',
              'GCC_ENABLE_CPP_EXCEPTIONS': 'YES'
            },
            'libraries' : ['-lpthread -lz -lc']
          }
        ],
        [
          'OS=="linux"', 
          {
            'libraries' : ['-lpthread -lz -lc -lrt']
          }
        ],
      ]
    }
  ]
}
