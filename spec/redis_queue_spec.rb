require 'redis_queue'

describe RedisQueue do
  let(:queue) { RedisQueue.new.tap { |q| q.clear } }

  it "pops pushed message" do
    queue.push "message"
    expect(queue.pop).to eq "message"
  end

  it "pops preserving order" do
    queue.push "message 1"
    queue.push "message 2"
    expect(queue.pop).to eq "message 1"
    expect(queue.pop).to eq "message 2"
  end

  it "pops preserving priority" do
    queue.push "message 1"
    queue.push "message 2", true
    queue.push "message 3", true
    queue.push "message 4", true
    queue.push "message 5"
    expect(queue.pop).to eq "message 2"
    expect(queue.pop).to eq "message 3"
    expect(queue.pop).to eq "message 4"
    expect(queue.pop).to eq "message 1"
    expect(queue.pop).to eq "message 5"
  end

  it "returns queue size" do
    queue.push "message 1"
    queue.push "message 2"
    expect(queue.size).to eq 2
  end

  it "returns queue list" do
    queue.push "message 1"
    queue.push "message 2"
    expect(queue.list).to eq ["message 1", "message 2"]
  end

  it "restarts queue" do
    queue.push "message 1"
    queue.done queue.pop
    queue.restart
    expect(queue.pop).to eq "message 1"
  end

  describe "in use list" do
    before do
      queue.push "message"
      queue.pop
    end

    it "returns size" do
      expect(queue.in_use_size).to eq 1
    end

    it "returns list" do
      expect(queue.in_use_list).to eq ["message"]
    end

    it "empties list when done" do
      queue.done "message"
      expect(queue.in_use_list).to eq []
    end

    it "empties list when failed" do
      queue.fail "message"
      expect(queue.in_use_list).to eq []
    end

    it "empties list when forgetting" do
      queue.forget "message"
      expect(queue.in_use_list).to eq []
    end
  end

  describe "failed list" do
    before do
      queue.push "message"
      queue.pop
      queue.fail "message"
    end

    it "returns size" do
      expect(queue.failed_size).to eq 1
    end

    it "returns list" do
      expect(queue.failed_list).to eq ["message"]
    end
  end

  describe "done list" do
    before do
      queue.push "message"
      queue.pop
      queue.done "message"
    end

    it "returns size" do
      expect(queue.done_size).to eq 1
    end

    it "returns list" do
      expect(queue.done_list).to eq ["message"]
    end
  end

  it "unpops message" do
    queue.push "message 1"
    queue.push "message 2"
    queue.unpop queue.pop
    expect(queue.pop).to eq  "message 1"
  end

  it "repushes message" do
    queue.push "message 1"
    queue.push "message 2"
    queue.repush queue.pop
    expect(queue.in_use_size).to eq 0
    expect(queue.pop).to eq  "message 2"
    expect(queue.pop).to eq  "message 1"
  end

  it "resets by putting used messages back to queue" do
    queue.push "message 1"
    queue.push "message 2"
    queue.push "message 3"
    queue.pop
    queue.pop
    queue.reset
    expect(queue.list).to eq  ["message 2", "message 1", "message 3"]
  end
end
