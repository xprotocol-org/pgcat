require 'pg'

describe 'DEALLOCATE handling' do
  it 'correctly handles explicit DEALLOCATE' do
    conn = PG.connect(
      host: '127.0.0.1',
      port: 6432,
      user: 'sharding_user',
      password: 'sharding_user',
      dbname: 'sharded_db'
    )

    conn.prepare("my_stmt", "SELECT 1")
    res = conn.exec_prepared("my_stmt", [])
    expect(res.getvalue(0, 0)).to eq('1')

    conn.exec("DEALLOCATE ALL")

    # Prepare again.
    conn.prepare("my_stmt", "SELECT 1")
    res = conn.exec_prepared("my_stmt", [])
    expect(res.getvalue(0, 0)).to eq('1')

    conn.close
  end
end
